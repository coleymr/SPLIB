<?php
/**
 * File Multithread.php - Run multiple window safe threads
 *
 * @category SPLIB
 * @package Multitasking
 * @copyright Copyright (c) 2014, Mark Coley
 * @version $Id$
 * @author Mark Coley
 */

/**
 * Class SPLIB_Multitasking_Thread - A class that implements windows safe thread
 *
 * @category SPLIB
 * @package Multitasking
 * @copyright Copyright (c) 2014, Mark Coley
 * @author Mark Coley
 */
class SPLIB_Multitasking_Thread
{
    /**
     * @access public
     * @var int $pref process reference
     */
	public $pref ;

	/*
	 * @access public
	 * var array $pipes standard IO pipes
	 */
	public $pipes; // stdio

	/**
	 * @access public
	 * @string buffer output buffer
	 */
	public $buffer;

	public $output;
	public $error;
	public $timeout;
	public $start_time;

	/**
	 * Thread::__constructor
	 *
	 * initialise a thread
	 *
	 * @access public
	 * @param integer $timeout timeout value in seconds
	 */
	function __construct($timeout)
	{
		$this->pref = 0;
		$this->buffer = "";
		$this->pipes = (array)NULL;
		$this->output = "";
		$this->error="";

		$this->start_time = time();
		$this->timeout = $timeout;
	}

	/**
	 * Thread::create
	 *
	 * initialise a thread
	 *
	 * @access public
	 * @param string $command command to be executed
	 * @param integer $timeout timeout value in seconds
	 * return Thread
	 */
	static function create ($command, $timeout) {
		$t = new AwLib_Threads_Thread($timeout);
		$descriptor = array (0 => array ("pipe", "r"), 1 => array ("pipe", "w"), 2 => array ("pipe", "w"));
		//Open the resource to execute $command
		$t->pref = proc_open($command,$descriptor,$t->pipes);
		//Set STDOUT and STDERR to non-blocking
		stream_set_blocking ($t->pipes[1], 0);
		stream_set_blocking ($t->pipes[2], 0);
		return $t;
	}

	/**
	 * Thread::isActive - See if the command is still active
	 *
	 * @access public
	 * return boolean
	 */
	function isActive () {
		$this->buffer .= $this->listen();
		$f = stream_get_meta_data ($this->pipes[1]);
		return !$f["eof"];
	}

	/**
	 * Thread::close - close the process
	 *
	 * @access public
	 * return integer termination status
	 */
    function close () {
		$r = proc_close ($this->pref);
		$this->pref = NULL;
		return $r;
	}

	/**
	 * Thread::tell - Send a message to the command running
	 *
	 * @access public
	 */
	function tell ($thought) {
		fwrite ($this->pipes[0], $thought);
	}

	/**
	 * Thread::listen
	 *
	 * Get the command output produced so far
	 *
	 * @access public
	 * return array $buffer
	 */
	function listen () {
		$buffer = $this->buffer;
		$this->buffer = "";
		while ($r = stream_get_contents($this->pipes[1])) {
			$buffer .= $r;
		}
		return $buffer;
	}

	/**
	 * Thread::getstatus
	 *
	 * Get the status of the current runing process
	 *
	 * @access public
	 * return array data about a process opened
	 */
	function getStatus(){
		return proc_get_status($this->pref);
	}

	/**
	 * Thread::isBusy
	 *
	 * See if the command is taking too long to run (more than $this->timeout seconds)
	 *
	 * @access public
	 * return integer timestamp
	 */
	function isBusy(){
		return ($this->start_time>0) && ($this->start_time+$this->timeout<time());
	}

	/**
	 * Thread::getError
	 *
	 * What command wrote to STDERR
	 *
	 * @access public
	 * return array $buffer
	 */
	function getError () {
		$buffer = "";
		while ($r = stream_get_contents ($this->pipes[2])) {
			$buffer .= $r;
		}
		return $buffer;
	}
}

/**
 * Class SPLIB_Multitasking_Thread - A class that implements windows safe multi-threaded applications
 * Wrapper for Thread class that uses proc_open to create multiple threading
 *
 * @category SPLIB
 * @package Multitasking
 * @copyright Copyright (c) 2014, Mark Coley
 * @author Mark Coley
 *
 * usage:
 * <pre>
 * set_time_limit(0);
 * include "Multithread.php";
 * $commands = array('ffmpeg -i '.$inputFile[0].' '.$outputFile[0].' 2>&1','ffmpeg -i '.$inputFile[0].' '.$outputFile[0].' 2>&1');
 * $threads = new AwLib_Threads_Multithread($commands,120);
 * $threads->run();
 * foreach ($threads->commands as $key=>$command){
 *	 echo "Command ".$command.":<br>";
 *	 echo "Output ".$threads->output[$key]."<br>";
 *	 echo "Error ".$threads->error[$key]."<br><br>";
 * }
 * </pre>
 */
class SPLIB_Multitasking_Multithread
{
	var $output;
	var $error;
	var $thread;
	var $commands = array();

	function __construct($commands, $timeout=0){
		$this->commands = $commands;

		foreach ($this->commands as $key=>$command){
			$this->thread[$key]=AwLib_Threads_Thread::create($command, $timeout);
			$this->output[$key]="";
			$this->error[$key]="";
		}
	}

	/**
	 * Thread::Multithread
	 *
	 * run threads
	 *
	 * @access public
	 * return array data about a processes
	 */
	function run(){
		$commands = $this->commands;
		//Cycle through commands
		while (count($commands)>0){
			foreach ($commands as $key=>$command){
				//Check if command is still active
				if ($this->thread[$key]->isActive()){
					//Get the output and the errors
					$this->output[$key].=$this->thread[$key]->listen();
					$this->error[$key].=$this->thread[$key]->getError();
					//Check if command is busy
					if ($this->thread[$key]->isBusy()){
						$this->thread[$key]->close();
						unset($commands[$key]);
					}
				} else {
					//gather results and free resources
					$this->output[$key].=$this->thread[$key]->listen();
					$this->error[$key].=$this->thread[$key]->getError();
					$this->thread[$key]->close();
					unset($commands[$key]);
				}
			}
		}
		return $this->output;
	}
}
