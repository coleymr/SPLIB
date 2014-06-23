<?php
set_time_limit(0);
include "Multithread.php";
$commands = array('ffmpeg -i '.$inputFile[0].' '.$outputFile[0].' 2>&1','ffmpeg -i '.$inputFile[0].' '.$outputFile[0].' 2>&1');
$threads = new SPLIB_Multitasking_Multithread($commands,120);
$threads->run();
foreach ($threads->commands as $key=>$command){
    echo "Command ".$command.":<br>";
    echo "Output ".$threads->output[$key]."<br>";
    echo "Error ".$threads->error[$key]."<br><br>";
}
?>