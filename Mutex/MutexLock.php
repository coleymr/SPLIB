<?php
/**
 * MutexLock
 *
 * Provides a class to place a lock on a mssql db resource
 * @author mark.coley
 * @package Mutex
 */
class MutexLock
{
	/**
	 * The resource handle to lock.
	 * @var string $key
	 */
	protected $handle;

	/**
	 * Lock timout value in milliseconds
	 * @var int $timeout
	 */
	protected $timeout;

	/**
	 * True if the resource lock has been established, false otherwise
	 * @var boolean $locked
	 */
	protected $locked;

	/**
	 * Lock mode can be one of these values: Shared, Update, Exclusive, IntentExclusive, IntentShared.
	 */
	protected $mode;

	/**
	 * Database handle resource
	 * @var object $db
	 */
	protected $db;

	/**
	 * Hold all errors
	 *
	 * @var array
	 */
	protected $errors = array();

	/*
	 * Debugging
	 */
	protected $debug;
	protected $debugFile;

	/**
	 * MutexLock::__construct
	 * Initialise a lock object.
	 *
	 * @param string $handle
	 * @param string $db
	 * @param int $timeout
	 * @param string $mode
	 * @return boolean
	 */
	function __construct($handle, $db, $config = array())
	{
	    // perform checks
	    try {
	        // initialise lock object
	        $this->init($config);

	        if (empty($handle)) throw new Exception(__METHOD__ . ": Unknown lock handle");
            $this->handle= $handle;

	        $arr = (array)$db;
	        if (empty($arr)) throw new Exception(__METHOD__ . ": Unknown database resource");
            $this->db = $db;

	    } catch (Exception $e) {
	        if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Error: ' . $e->getMessage() . PHP_EOL, 3, $this->debugFile);
	        $this->errors[] = $e->getMessage();
	        return false;
	    }
	    return true;
	}

	/**
	 * MutexLock::init
	 * Initialize preferences
	 *
	 * This function will take an associative array of config values and
	 * will initialize the class variables using them.
	 *
	 * Example use:
	 *
	 * <pre>
	 * $config['timeout'] = 60;
	 * $config['mode'] = 'Shared';
	 * $config['debug'] = TRUE;
	 * $lock = new MutexLock('__lock_handle__', $db, $config);
	 * </pre>
	 *
	 * @param array $config values as associative array
	 * @return void
	 */
	public function init($config = array())
	{
	    $this->_clear();
	    if (!empty($config) && is_array($config)) {
	        foreach ($config as $key => $value) {
	            if (property_exists('MutexLock', $key)) {
	               $this->_set($key, $value);
	            }
	        }
	    }
	    if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': initialised lock object =' . print_r($this, 1) . PHP_EOL, 3, $this->debugFile);
	}

	/**
	 * MutexLock::_clear
	 * Clear Everything
	 *
	 * Clears all the properties of the class and sets the object to
	 * the beginning state. Very handy if you are doing subsequent calls
	 * with different data.
	 *
	 * @return void
	 */
	protected function _clear()
	{
	    // Set the defaults
	    $this->handle = '__lock_handle__';
	    $this->db = NULL;
	    $this->timeout = 0;
	    $this->locked = FALSE;
	    $this->mode = 'Exclusive';

	    // Set the config details
	    $this->errors = array();
	    $this->debug = FALSE;
	    $this->debugFile = 'c:\\temp\MutexLock_debug.log';
	}

	/**
	 * MutexLock::_destroy
	 *
	 * Cleanup lock
	 */
	function __destruct()
	{
	    if( $this->locked == TRUE )
	        $this->unlock();
	}

	/**
	 * MutexLock::lock
	 * Places a lock on a resource and returns locked state
	 *
	 * @return boolean
	 */
	public function lock()
	{
	    try {
	        if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Attempting to lock: ' . $this->handle . PHP_EOL, 3, $this->debugFile);
    		$sql = "BEGIN TRAN
DECLARE @returnCode INT
EXEC @returnCode = sp_getapplock 
	@Resource = '{$this->handle}', 
	@LockMode = '{$this->mode}', 
	@LockOwner = 'Session', 
	@LockTimeout = {$this->timeout},
	@DbPrincipal  = 'public'
SELECT @returnCode, resource_type, request_mode, resource_description 
FROM sys.dm_tran_locks WHERE resource_type='APPLICATION' AND resource_description LIKE '%{$this->handle}%'
IF @returnCode NOT IN (0, 1) 
BEGIN 
	RAISERROR ( 'Unable to acquire Lock', 16, 1 ) 
	ROLLBACK TRAN
	RETURN
END
ELSE
BEGIN
	PRINT 'LOCK ACQUIRED'
END
COMMIT";//Calling SP

    		$stmt = $this->_query($sql);
    		$rows = $this->_fetchAll($stmt);
    		$num_rows = count($rows);
    		
    		if ($num_rows > 0) $this->locked = TRUE;
	    } catch (Exception $e) {
            if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Error: ' . $e->getMessage() . PHP_EOL, 3, $this->debugFile);
	        $this->errors[] = $e->getMessage();
            $this->locked = FALSE;
	    }
	    if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Returning: ' . $this->locked . PHP_EOL, 3, $this->debugFile);
		return $this->locked;
	}

	/**
	 * MutexLock::unlock
	 * Release a lock on an resource and returns the locked state
	 *
	 * @return boolean
	 */
	public function unlock()
	{
	    try {
	        if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Attempting to unlock: ' . $this->handle . PHP_EOL, 3, $this->debugFile);
    		$sql = "BEGIN
DECLARE @returnCode INT
EXEC @returnCode = sp_releaseapplock 
	@Resource = '{$this->handle}',
	@DbPrincipal = 'public',
	@LockOwner = 'Session' 
SELECT @returnCode, resource_type, request_mode, resource_description 
FROM sys.dm_tran_locks 
WHERE resource_type='APPLICATION' AND resource_description LIKE '%{$this->handle}%'
PRINT 'LOCK RELEASED'
	--COMMIT TRAN
END";
    		$stmt = $this->_query($sql);
    		$rows = $this->_fetchAll($stmt);
    		$num_rows = count($rows);
    		if (($num_rows > 0) == TRUE) $this->locked = FALSE; else $this->locked = TRUE;
    		
	    } catch (Exception $e) {
            if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Error: ' . $e->getMessage() . PHP_EOL, 3, $this->debugFile);
            $this->errors[] = $e->getMessage();
            $this->locked = FALSE;
	    }
    	return $this->locked;
	}

	/**
	 * MutexLock::isFree
	 * Returns an indication if resource is free to lock
	 *
	 * @return boolean
	 */
	public function isFree()
	{
	    try {
	        $num_rows = 0;
    		$sql = "SELECT resource_type, request_mode, resource_description FROM sys.dm_tran_locks WHERE resource_type='APPLICATION' AND resource_description LIKE '%{$this->handle}%';";
    		$stmt = $this->_query($sql);
    		$rows = $this->_fetchAll($stmt);
            $num_rows = count($rows);

            if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': num_rows=' . $num_rows . PHP_EOL, 3, $this->debugFile);

    	} catch (Exception $e) {
            if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': Error: ' . $e->getMessage() . PHP_EOL, 3, $this->debugFile);
            $this->errors[] = $e->getMessage();
            $this->locked = FALSE;
        }
        return ($this->locked || $num_rows > 0)? FALSE : TRUE;
	}

	/**
	 * MutexLock:: _query
	 * Preparesand executes a SQL and reurns a statement object
	 *
	 * @param string $statement
	 * @returns mixture
	 */
	protected function _query($statement)
	{
	    if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': SQL=' . $statement . PHP_EOL, 3, $this->debugFile);
	  //  @$this->db->getConnection();
	    try {
	        $stmt = $this->db->prepare($statement);
	        $stmt->execute();
	    } catch (Exception $e) {
	        throw new Exception("Could not query database! " . $e->getMessage());
	    }
	    return $stmt;
	}

	protected function _fetchAll($stmt)
	{
	    $data = $stmt->fetchAll();
	    if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': data=' . print_r($data, 1) . PHP_EOL, 3, $this->debugFile);	     
	    //@$this->db->closeConnection();
	    return $data;
	}

	protected function _fetch($stmt)
	{
	    $data = $stmt->fetch();
	    if ($this->debug) error_log(date('d-m-Y: H:i:s') . ': ' . __FUNCTION__ . ': data=' . print_r($data, 1) . PHP_EOL, 3, $this->debugFile);
	    //@$this->db->closeConnection();
	    return $data;
	}

	/**
	 * MutexLock::__get
	 * Return held lock data
	 *
	 * @param string $name
	 * @return mixture
	 */
	public function __get($name) {
		return $this->$name;
	}

	/**
	 * MutexLock::__set
	 * Set lock data
	 *
	 * @param string $name
	 * @param mixture $value
	 * @return void
	 */
	protected function _set($name, $value) {
	    switch ($name) {
	        case 'handle':
	            if (empty($value)) throw new Exception(__METHOD__ . ": Unknown lock handle");
	            break;
	        case 'db':
	            $arr = (array)$db;
	            if (empty($arr)) throw new Exception(__METHOD__ . ": Unknown database resource");
	        $this->db = $db;
	        case 'mode':
	            if (!in_array($value,array('Shared', 'Update', 'Exclusive', 'IntentExclusive', 'IntentShared'))) $this->mode = 'Exclusive';
                break;
	        case 'timeout':
	            if (! is_int($value)) $this->timeout = 0;
	            break;
	        default:
            $this->$name = $value;
	    }
	}
}