<?php

//namespace Tools\Console\Commands;

require 'HotFolderWatcher.php';

$pdo = new PDO('mysql:host=192.168.1.113;dbname=tools', 'joyce', 'yu1992720');

$folderWatch = new HotFolderWatcher('../storage/M_HotFolder/', $pdo);

$folderWatch->checkFiles();

$folderWatch->generateAndExportReports();