<?php
// PHP Data Objects(PDO) Sample Code:
try {
    $conn = new PDO("sqlsrv:server = tcp:dfhntz.database.windows.net,1433; Database = testdb", "dfhntz", "Azubitably");
    $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
}
catch (PDOException $e) {
    print("Error connecting to SQL Server.");
    die(print_r($e));
}

// SQL Server Extension Sample Code:
$connectionInfo = array("UID" => "dfhntz@dfhntz", "pwd" => "{your_password_here}", "Database" => "testdb", "LoginTimeout" => 30, "Encrypt" => 1, "TrustServerCertificate" => 0);
$serverName = "tcp:dfhntz.database.windows.net,1433";
$conn = sqlsrv_connect($serverName, $connectionInfo);

