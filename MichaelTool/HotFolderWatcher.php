<?php

//namespace Tools\Console\Commands;

class HotFolderWatcher
{
    private $inputFolder;
    private $files = [];
    private $pdo;
    private $hasPSAA1 = false;
    private $hasELP = false;
    private $hasPSAA2= false;
    private $hasRate = false;

    public function __construct($dir, $pdo)
    {
        $this->inputFolder = $dir;
        $this->pdo = $pdo;
    }

    // Check if the files are changed
    public function checkFiles()
    {
        if (is_dir($this->inputFolder)) {
            if ($dh = opendir($this->inputFolder)) {
                while (($filename = readdir($dh)) !== false) {
                    if ($filename != "." && $filename != "..") {
                        $this->files[] = $filename;
                    }
                }
                closedir($dh);
            }
        }

        if (in_array("PSAA1.CSV", $this->files)) {
            if ((time() - filectime($this->inputFolder."PSAA1.CSV")) < 60) exit;
            $this->hasPSAA1 = true;
        }

        if (in_array("PSAA2.CSV", $this->files)) {
            if ((time() - filectime($this->inputFolder."PSAA2.CSV")) < 60) exit;
            $this->hasPSAA2 = true;
        }

        if (in_array("ELP.CSV", $this->files)) {
            if ((time() - filectime($this->inputFolder."ELP.CSV")) < 60) exit;
            $this->hasELP = true;
        }

        if (in_array("Rate.csv", $this->files)) {
            if ((time() - filectime($this->inputFolder."Rate.csv")) < 60) exit;
            $this->hasRate = true;
        }
    }

    public function generateAndExportReports()
    {
        if ($this->hasELP && $this->hasPSAA1) {
            $this->generateReport1();
            $this->exportReport('Report1');
        }

        if ($this->hasELP && $this->hasPSAA2) {
            $this->generateReport2();
            $this->exportReport('Report2');
        }

        if ($this->hasELP && $this->hasRate) {
            $this->generateReport3();
            $this->exportReport('Report3');
        }
    }

    private function exportReport($table)
    {
        $query = "SELECT * FROM $table";
        $statement = $this->pdo->prepare($query);
        $statement->execute();
        $records = $statement->fetchAll(PDO::FETCH_ASSOC);

        if(!is_array($records[0])) {
            echo "Sorry, $table can't be exported.";
        }

        $header = array_keys($records[0]);

        $fh = fopen($this->inputFolder.$table.".csv", "w");

        if ($table === 'Report1') {
            fputcsv($fh, null);
        } else {
            fputcsv($fh, $header);
        }

        foreach($records as $record) {
            fputcsv($fh, $record);
        }

        fclose($fh);
    }

    // Process files
    private function generateReport1()
    {
        // read the csv file and insert it into the tables line by line
        $this->importCSV("ELP",".CSV");
        $this->importCSV("PSAA1",".CSV");


        $this->dropTable("Temp_ELP");
        $query = "  CREATE TABLE Temp_ELP
                    SELECT 
                        `Facility Type`,
                        CONCAT(`Facility Type`,' ',City) as `Facility Name`,
                        `Entry Point`,
                        Copies,
                        `Gross Wgt`,
                        `Pallet Count`,
                        Courier
                    FROM ELP";
        $this->runQuery($query);

        $query = "  DELETE FROM Temp_ELP
                    WHERE `Facility Name` = 'SCF DALLAS' 
                    OR `Facility Name` = 'NDC DALLAS'
                    OR `Facility Name` = 'SCF NORTH TEXAS'
                    OR `Facility Name` = 'SCF FORT WORTH'";
        $this->runQuery($query);

        $this->dropTable("Temp_PSAA1");
        $query = "  CREATE TABLE Temp_PSAA1
                    SELECT 
                          HDRLicensedUsersJobNumber, 
                          HDRJobNameTitleAndIssue, 
                          MPUProcessingCategory, 
                          ScheduledInductionDate,
                          EPDeliveryLocaleKey,
                          FLOOR(EPDeliveryZip4 / 10000) as EPDeliveryZip,
                          ContainerShipDate
                    FROM PSAA1
                    GROUP BY 
			              EPDeliveryLocaleKey,
                          HDRLicensedUsersJobNumber, 
                          HDRJobNameTitleAndIssue, 
                          MPUProcessingCategory, 
                          ScheduledInductionDate,
                          EPDeliveryZip,
                          ContainerShipDate";
        $this->runQuery($query);

        $this->dropTable("Report1");
        $query = "  CREATE TABLE Report1
                    SELECT
	                  t1.`Entry Point` as `Destination postal code`,
	                  t1.`Gross Wgt` as `Weight`,
	                  t2.`ContainerShipDate` as `Ship Date`,
	                  t1.`Courier` as `SCAC`
                    FROM Temp_ELP AS t1 JOIN Temp_PSAA1 AS t2
                    ON t1.`Entry Point` = t2.EPDeliveryZip";
        $this->runQuery($query);

        $query = "  ALTER TABLE Report1
                    ADD COLUMN `Origin postal code` VARCHAR(255) NOT NULL DEFAULT '75244' FIRST,
                    ADD COLUMN `Freight Class` VARCHAR(255) NOT NULL DEFAULT '77.5' AFTER `Weight`,
                    ADD COLUMN `(P)ick or (D)rop` VARCHAR(255) NOT NULL DEFAULT 'P' AFTER `Freight Class`,
                    ADD COLUMN `Shipment Identifier(numeric)` VARCHAR(255) AFTER `Ship Date`,
                    ADD COLUMN `NMFC` VARCHAR(255) AFTER `Shipment Identifier(numeric)`,
                    ADD COLUMN `Discount Tariff` VARCHAR(255) AFTER `NMFC`";
        $this->runQuery($query);

        $query = "  UPDATE Report1
                    SET SCAC = 'FXFE'
                    WHERE SCAC LIKE 'F';

                    UPDATE Report1
                    SET SCAC = ''
                    WHERE SCAC LIKE 'C';";
        $result = $this->runQuery($query);
        if ($result)
            echo "report1 generate succeed<br>";
        else
            echo "report1 generate failed<br>";
    }

    private function generateReport2()
    {
        // read the csv file and insert it into the table line by line
        $this->importCSV("ELP",".CSV");
        $this->importCSV("PSAA2",".CSV");

        $query = "
                    DROP TABLE IF EXISTS Temp_ELP2;
                    CREATE TABLE Temp_ELP2
                    SELECT 
                        `Facility Type`,
                        CONCAT(`Facility Type`,' ',City) as `Facility Name`,
                        `Entry Point`,
                        Copies,
                        `Gross Wgt`,
                        `Pallet Count`
                    FROM ELP;
                    
                    DELETE FROM Temp_ELP2
                    WHERE `Facility Name` = 'SCF DALLAS' 
                    OR `Facility Name` = 'NDC DALLAS'
                    OR `Facility Name` = 'SCF NORTH TEXAS'
                    OR `Facility Name` = 'SCF FORT WORTH';
                    
                    DROP TABLE IF EXISTS Temp_PSAA2;
                    CREATE TABLE Temp_PSAA2
                    SELECT HDRLicensedUsersJobNumber, 
                                 HDRJobNameTitleAndIssue, 
                                 MPUProcessingCategory, 
                                 ScheduledInductionDate,
                                 EPDeliveryLocaleKey,
                                 FLOOR(EPDeliveryZip4 / 10000) as EPDeliveryZip
                    FROM PSAA2
                    GROUP BY 
                                 EPDeliveryLocaleKey,
                                 HDRLicensedUsersJobNumber, 
                                 HDRJobNameTitleAndIssue, 
                                 MPUProcessingCategory, 
                                 ScheduledInductionDate,
                                 EPDeliveryZip;
                    
                    DROP TABLE IF EXISTS Report2;
                    CREATE TABLE Report2
                    SELECT
                        t1.`Facility Type`,
                        t2.EPDeliveryLocaleKey as `USPS  / FAST Facility Key`,
                        t1.`Facility Name`,
                        t1.`Entry Point` as `Zip Code`,
                        t2.HDRJobNameTitleAndIssue as `Job Name`,
                        t2.HDRLicensedUsersJobNumber as `Sales Order Number`,
                        t2.ScheduledInductionDate as `Latest Delivery/Appointment Date`,
                        t1.Copies as Quantity,
                        t1.`Gross Wgt` as Weight,
                        SUBSTRING(t2.MPUProcessingCategory, 1, 1) as `Mail Processing Category`,
                        t1.`Pallet Count` as Skids,
                        t2.HDRLicensedUsersJobNumber as `Custom Ref#1`,
                        t2.HDRJobNameTitleAndIssue as `Custom Ref#2`
                    FROM Temp_ELP2 AS t1 JOIN Temp_PSAA2 AS t2
                    ON t1.`Entry Point` = t2.EPDeliveryZip;
                    
                    ALTER TABLE Report2
                    ADD COLUMN `Address #1` varchar(255) AFTER `Facility Name`,
                    ADD COLUMN `Address #2` varchar(255) AFTER `Address #1`,
                    ADD COLUMN City varchar(255) AFTER `Address #2`,
                    ADD COLUMN State varchar(255) AFTER City,
                    ADD COLUMN `Earliest In-Home` varchar(255) AFTER `Sales Order Number`,
                    ADD COLUMN `Latest In-Home` varchar(255) AFTER `Earliest In-Home`,
                    ADD COLUMN `Earliest Delivery Date` varchar(255) AFTER `Latest In-Home`,
                    ADD COLUMN `Latest Delivery/Appointment Time` varchar(255) AFTER `Latest Delivery/Appointment Date`,
                    ADD COLUMN `Version (Customer Ref#3)` varchar(255) AFTER `Mail Processing Category`,
                    ADD COLUMN Sacks int AFTER Skids,
                    ADD COLUMN Trays int AFTER Sacks,
                    ADD COLUMN `Custom Ref#4` varchar(255) AFTER `Custom Ref#2`,
                    ADD COLUMN `Custom Ref#5` varchar(255) AFTER `Custom Ref#4`,
                    ADD COLUMN `Custom Ref#6` varchar(255) AFTER `Custom Ref#5`,
                    ADD COLUMN `Custom Ref#7` varchar(255) AFTER `Custom Ref#6`,
                    ADD COLUMN `Target Pickup Date` varchar(255) AFTER `Custom Ref#7`;
                    
                    UPDATE Report2, LOCkey
                    SET Report2.`USPS  / FAST Facility Key` = LOCkey.`Dropsite Key`
                    WHERE SUBSTRING(Report2.`USPS  / FAST Facility Key`, 5) = SUBSTRING(LOCkey.`Dropsite Key`, 3);";

        $result = $this->runQuery($query);
        if ($result)
            echo "report2 generate succeed<br>";
        else
            echo "report2 generate failed<br>";
    }

    private function generateReport3()
    {
        $this->importCSV("ELP",".CSV");
        $this->importCSV("Rate",".csv");

        $query = "
                DROP TABLE IF EXISTS Report3;
                CREATE TABLE Report3
                SELECT
                    t1.`City`,
                    t1.`State`,
                    t1.`Facility Type`,
                    t1.`Job ID`,
                    t1.`Entry Point`,
                    t1.`Pallet Count`,
                    t1.`Gross Wgt`,
                    t1.`Copies`,
                    t1.`Tray Count`,
                    t1.`Postage Savings`,
                    t2.`Net Charge` as `Freight Cost`,
                    t1.`Entry Postage`,
                    t1.`Local Postage`,
                    t1.`Net Savings`,
                    t2.`Carrier`,
                    t2.`transit time`
                FROM ELP AS t1 LEFT JOIN Rate AS t2
                ON t1.`Entry Point` = t2.`Destination`;
        ";
        $result = $this->runQuery($query);
        if ($result)
            echo "report3 generate succeed<br>";
        else
            echo "report3 generate failed<br>";
    }

    private function importCSV($table, $suffix)
    {
        $records = array_map('str_getcsv',file($this->inputFolder.$table.$suffix));
        //die(var_dump($records));
        $fields = $records[0];
        array_shift($records);

        // DROP TABLE in database
        $this->dropTable($table);

        // CREATE TABLE in database
        $this->createTable($table, $fields);

        // INSERT INTO TABLE line by line
        $this->insertRecordsIntoTable($table, $fields, $records);
    }

    private function dropTable($table)
    {
        $sql = sprintf(
            'DROP TABLE IF EXISTS %s',
            $table
        );
        $this->runQuery($sql);
    }

    private function createTable($table, $fields)
    {
        $sql = sprintf(
            'CREATE TABLE %s (%s)',
            $table,
            "`".implode("` VARCHAR(255), `", $fields)."` VARCHAR(255)"
        );
        $this->runQuery($sql);
    }

    private function insertRecordsIntoTable($table, $fields, $records)
    {
        foreach ($records as $record) {
            $sql = sprintf(
                'INSERT INTO %s (%s) VALUES (%s)',
                $table,
                "`".implode("`, `", $fields)."`",
                str_repeat("?, ", (count($fields)-1))."?"
            );
            $this->runQuery($sql, $record);
        }
    }

    private function runQuery($sql, $record = [])
    {
        try {
            $statement = $this->pdo->prepare($sql);
            return $statement->execute($record);
        } catch (Exception $e) {
            die($e->getMessage());
        }
    }
}