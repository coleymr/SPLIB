<?php
	ini_set('display_errors', 1);
	error_reporting(E_ALL);

	$host = "10.0.0.85";
	$username = "sa";
	$password = "autoweb";
	$dbname = "autoweb";
	$dsn = "mssql:$host;dbname=$dbname"; 

	//connection to the database
	$db = new PDO($dsn, $username, $password);

	$config = array();
  	$config['timeout'] = 60;
  	$config['mode'] = 'Exclusive';
  	$config['debug'] = TRUE;
  	$mutextLock = new MutexLock('__lock_handle__', $db, $config);

	if($mutexLock->isFree() === TRUE) {
		$mutextLock->lock();
		echo 'Lock acquired' . PHP_EOL;
		sleep(5);
		if($mutexLock->isFree() !== TRUE) {
        	$lock->unlock();
			echo 'Lock removed' . PHP_EOL;
        }
	} else {
		echo 'Lock exsists' . PHP_EOL;
	}
?>