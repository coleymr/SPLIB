#!/usr/bin/perl
# MySQL_check_replication.pl
# Version 1.0.0. beta
# Last Modified 2012-07-25
# Originally written by: Mark R Coley
#
# This script will check that check that MySQL Master and Slave replication is working
# If master is not available it will promote a slave and reconfigure other
# slaves to point to the new master.
#
# This script follows the instructions outlined in the MySQL Documentation
# 16.3.6. Switching Masters During Failover
# ( http://dev.mysql.com/doc/refman/5.5/en/replication-solutions-switch.html )
#
# THIS PROGRAM IS PROVIDED "AS IS"
# WITHOUT WARRANTY EXPRESSED OR IMPLIED.
#
# Assumptions:
#  1) You actually read 15.3.6. Switching Masters During Failover to make sure
#      your configuration of your masters and slaves are correct.
#  2) Run your PR:M/B servers with the --log-bin option and without --log-slave-updates.
#  3) User specified on the command line has an account on all servers.
#  4) User specified on the command line has sufficient priviliges to issue REP changes.
#  5) MySQL accounts are replicated or are the same on all PR master servers.
#
# REQUIRED TEXT FILE: drupal_db_servers.dat
# There must be at lease two MySQL servers in this file.
# There must be at least: one (PR:B) as master (CR:M) and; one (PR:B) as a slave (CR:S).
# To promote you must have at least TWO servers capable of being masters.
#
#
# drual_db_servers.dat format is:
#   Host : Port : User : Password : PR : CR
#
# Example drupal_db_servers.dat:
#   test01:3306:repslave:password:B:M
#   test02:3306:repslave:password:B:S
#   test03:3306:repslave:password:S:S
#   test04:3306:repslave:password:S:S
#   test05:3306:repslave:password:B:S
#
#
# Host: The host name or IP address of the MySQL server.
# Port: The port number of the MySQL server.
# User: The MySQL replication account of this host to log into the master with.
# Password: The MySQL replication accounts password.
# Permissible Role (PR):
#   B = Can run as either Master or Slave
#   M = Master Only
#   S = Slave Only
#   If it's not B,M, or S then host will be ignored and not processed.
#
# Current Role (CR):
#   M = Master (there can only be ONE current master server!)
#   S  = Slave
#   F = Failed/Error (something went wrong during promotion).
#
#
# REQUIRED TEXT FILE: drupal_fe_servers.dat
# There must be atleast one enabled drupal front-end web client in this file.
#
# drual_fe_servers.dat format is:
#   Host : User : ConfigPath : Enabled
#
# Example drupal_fe_servers.dat:
#       client01:root:/var/www/html/sites/default/:0
#       client02:root:/var/www/html/sites/default/:1
#       client03:root:/var/www/html/sites/default/:1
#
#
# Example execution:
# For testing:
#    perl MySQL_check_replication.pl --server=PrimaryMasterDBServer --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --wait_time=5 --test
# For real:
#    perl MySQL_check_replication.pl --server=PrimaryMasterDBServer --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat --wait_time=5
#
#

# libs
use strict;
use warnings;
use DBI;
use File::Basename;
use Cwd;
use POSIX qw(tmpnam);
use Data::Dumper;

# globals
my %Alerts;
my %DEFAULTS;
$DEFAULTS{ "WAIT_TIME" } = 5;			#5 secs
$DEFAULTS{ "MAX_CONNECTIONS" } = 251;
$DEFAULTS{ "MIN_UPTIME" } = 3600;		#1 hour
$DEFAULTS{ "MIN_SECONDS_BEHIND_MASTER" } = 120;	#2 mins
$DEFAULTS{ "QUIET" } = 0;
$DEFAULTS{ "TESTRUN" } = 0;
#$DEFAULTS{ "MAILTO" } = 'alert@exelit.com';	#live
$DEFAULTS{ "MAILTO" } = 'markr.coley@gmail.com,dom@contactmusic.com';#dev
$DEFAULTS{ "ALERT_MASTER_F" } = 30;		#30 secs
$DEFAULTS{ "ALERT_MASTER_R" } = 300;		#5 mins
$DEFAULTS{ "ALERT_MASTER_M" } = 30;		#30 secs
$DEFAULTS{ "ALERT_SLAVE_F" } = 30;		#30 secs
$DEFAULTS{ "ALERT_SLAVE_R" } = 300;		#5 mins
$DEFAULTS{ "ALERT_SLAVE_S" } = 30;		#30 secs
$DEFAULTS{ "ALERT_REPLICATION_FAIL" } = 600;	#10 mins	
$DEFAULTS{ "MAX_CONNECTION_RETRIES" } = 3;	#3 DB connections

my %WebClients;
my %ServerStatus;
my %ReplicationStatus;
my %ServerPort;
my %ServerMasterUser;
my %ServerMasterPass;
my %ServerPermRole;
my %ServerCurRole;
my %ServerOldCurRole;
my %FEServerUser;
my %FEServerConfigPath;
my %FEServerStatus;
my %ServerConn;
my %DBSlaves;
my %FailedDBSlaves;
my $ConfigCreated;
my $ErrorCount;
my $BothCount;
my $MasterCount;
my $SlaveCount;
my $OldMaster;
my %CLP;
my $TmpDBDataFile;
my $start_time;
my $end_time;
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);

while ( 1 ) {
	
	# var
	#%Alerts=();
	%WebClients=();
	%ServerStatus=();
	%ReplicationStatus=();
	%ServerPort=();
	%ServerMasterUser=();
	%ServerMasterPass=();
	%ServerPermRole=();
	%ServerCurRole=();
	%ServerOldCurRole=();
	%FEServerUser=();
	%FEServerConfigPath=();
	%FEServerStatus=();
	%ServerConn=();
	%DBSlaves=();
	%FailedDBSlaves=();
	%CLP=();
	$ConfigCreated = 0;
	$ErrorCount = 0;
	$BothCount = 0;
	$MasterCount = 0;
	$SlaveCount = 0;
	$OldMaster = "";
	$TmpDBDataFile = tmpnam();

	# Parse the command line arguments
	ParseCommandLine();

	# pause
	sleep $CLP{ 'WAIT_TIME' };

	# set start time
	$start_time = time;

	LogIt( "ALERTS " .Dumper( %Alerts ) );

	# Load the DB server list.
	LoadDBServers ($CLP{ 'DBCONFIGFILE' });

	# Load the FE server list.
	LoadFEServers ($CLP{ 'FECONFIGFILE' });

	# Validation
	Validate();

	# Step 1 - Connect to all the Database servers.
	ConnectToDBS();

	# Step 2 - Check for any known online slave(s) that may of failed
	CheckFailedSlaves (%DBSlaves);

	# Step 3 - Check for any known failed slave(s)that may have comeback online
	CheckBackOnlineSlaves (%FailedDBSlaves);
	
	# Step 4 - Check replication status
	CheckReplicationStatus();

	# Step 5 - Check for failed master
	CheckFailedMaster ($OldMaster, %DBSlaves);

	# Step 6 - Check for master that has comeback online
	CheckBackOnlineMaster ($OldMaster, %DBSlaves);

	# Close Mysql connections
	DisconnectFromDBS();

	# set end time 
	$end_time = time;

	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime( $start_time );
	$year += 1900;
	$mon += 1;
	LogIt( "===========================================================" );
	LogIt( "Process Started        : ". sprintf "%02d-%02d-%04d %02d:%02d:%02d", $mday, $mon, $year, $hour, $min, $sec );
	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime( $end_time );
	LogIt( "Process Finished       : ". sprintf "%02d-%02d-%04d %02d:%02d:%02d", $mday, $mon, $year, $hour, $min, $sec );
	LogIt( "===========================================================" );
	LogIt( " ");

}
#exit( 0 );


##############################################################################
# Subroutines
#

# Show the programs usage.
sub Usage
{
	print( "\n" );
	print( "Usage: MySQL_check_replication.pl --server=PrimaryMasterDBServer --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat\n" );
	print( " --server=, -s=        (r) the primary master server.\n" );
	print( " --user=, -u=          (r) a MySQL user with privileges to make rep changes.\n" );
	print( " --password=, -p=      (r) the password for --user.\n" );
	print( " --dbconfigfile=, -d=  (r) the drupal database config file.\n" );
    print( " --feconfigfile=, -f=  (r) the drupal database config file.\n" );
    print( " --configfile=, -c=    (r) config file.\n" );
	print( " --wait_time=, -w      (o) number of seconds between program executions (default 5).\n" );
	print( " --test, -t            (o) perform a test run altering nothing.\n" );
	print( " --quiet, -q           (o) quiet output (outputs nothing).\n" );
	print( " --help, -h, -?        (o) this usage information.\n" );
} # Usage


sub UsageHint
{
	print( "use --help for help or read the comments at the top of this program.\n" );
} # UsageHint


# Log items to screen (or a file if you add the code).
sub LogIt
{
	if ( $CLP{ "QUIET" } == 0 ) {
		my ($lsec,$lmin,$lhour,$lmday,$lmon,$lyear,$lwday,$lyday,$lisdst) = localtime;
		$lyear += 1900;
        $lmon += 1;		
		my $logtime =  sprintf ("%02d-%02d-%04d %02d:%02d:%02d", $lmday, $lmon, $lyear, $lhour, $lmin, $lsec);
		print( "[$logtime] $_[0]\n" );
	}
} # LogIt


# Log SQL Errors.
sub LogSQLError
{
	LogIt( "$_[0]! A SQL error occured." );
	if ( $_[1] )
	{
		LogIt( "The SQL statement was:" );
		LogIt( "$_[1]" );
	}
	LogIt( "The error was:" );
	LogIt( "$DBI::errstr" );
} # LogSQLError

# Send an alert via email
sub AlertIt
{
	my ($machine, $type, $subject, $text) = @_;
	if (! defined( $machine ) || $machine eq ""
                || ! defined( $type ) || $type eq ""
                || ! defined( $subject ) || $subject eq ""
                || ! defined( $text ) || $text eq "" ) { return; }

	# strip chars
	$subject  =~ s/"//g;
	$subject  =~ s/'//g;
	$subject  =~ s/!//g;
	$subject  =~ s/\(//g;
	$subject  =~ s/\)//g;
	$text  =~ s/"//g;
	$text  =~ s/'//g;
	$text  =~ s/!//g;
	$text  =~ s/\(//g;
	$text  =~ s/\)//g;

	
	if ( $CLP{ "TESTRUN" } == 0) {
		my $logtime = time;
		if ( $Alerts{ $machine }{ $type } ) {
			if ( $logtime < ($Alerts{ $machine }{ $type } + $DEFAULTS{ $type } ) ) { return ; }
		}
		LogIt( "Sending alert $subject: $text to $DEFAULTS{ 'MAILTO' }." );	
		$Alerts{ $machine }{ $type } = $logtime;

		my ($lsec,$lmin,$lhour,$lmday,$lmon,$lyear,$lwday,$lyday,$lisdst) = localtime;
		$lyear += 1900;
		$lmon += 1;
		my $logtimetxt =  sprintf ("%02d-%02d-%04d %02d:%02d:%02d", $lmday, $lmon, $lyear, $lhour, $lmin, $lsec);
		$text = "[$logtimetxt]" . $text;

		my $exe_cmd = "echo '$text' | mail -s '$subject' $DEFAULTS{ 'MAILTO' }";
		my $exec_result = `$exe_cmd 2>&1`;
		if ($? != 0) {
			LogIt( "Failed to send alert $subject: $text to $DEFAULTS{ 'MAILTO' }." );
    	}
	} # TESTRUN			
} # AlertIt

# Parse the command line arguments
# perl MySQL_check_replication.pl --server=PrimaryMasterDBServer --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --test
sub ParseCommandLine
{

	foreach my $arg ( @ARGV ) {
		my @mField= split( /=/, $arg );

		if ( uc( $mField[0] ) eq "--HELP" || uc( $mField[0] ) eq "-H" || $mField[0] eq "-?") {
			Usage();
			exit( 1 );
		}

		if ( uc( $mField[0] ) eq "--SERVER" || uc( $mField[0] ) eq "-S" ) {
    		$CLP{ "SERVER" } = $mField[1];
      	}
	
		if ( uc( $mField[0] ) eq "--USER" ||  uc( $mField[0] ) eq "-U" ) {
      		$CLP{ "USER" } = $mField[1];
     	}

		if ( uc( $mField[0] ) eq "--PASSWORD" || uc( $mField[0] ) eq "-P" ) {
			$CLP{ "PASSWORD" } = $mField[1];
		}

		if ( uc( $mField[0] ) eq "--DBCONFIGFILE" ||  uc( $mField[0] ) eq "-D" ) {
			$CLP{ "DBCONFIGFILE" } = $mField[1];
    	}

  		if ( uc( $mField[0] ) eq "--FECONFIGFILE" ||  uc( $mField[0] ) eq "-F" ) {
     		 $CLP{ "FECONFIGFILE" } = $mField[1];
    	}
		
		if ( uc( $mField[0] ) eq "--WAIT_TIME" || uc( $mField[0] ) eq "-W" ) {
			$CLP{ "WAIT_TIME" } = $mField[1];
		}

		if ( uc( $mField[0] ) eq "--QUIET" || uc( $mField[0] ) eq "-Q" ) {
			$CLP{ "QUIET" } = 1;
		}

		if ( uc( $mField[0] ) eq "--TEST" || uc( $mField[0] ) eq "-T" ) {
			$CLP{ "TESTRUN" } = 1;
		}

	}
	# Do we have a SERVER?
	if ( ! $CLP{ "SERVER" } || $CLP{ "SERVER" }  eq ""  ) {
       	LogIt( "FATAL! You must provide a valid MySQL server that is configured in $CLP{ 'DBCONFIGFILE' }." );
       	UsageHint();
       	exit( 1);
	}
		
	# Do we have a USER?
	if ( ! $CLP{ "USER" } || $CLP{ "USER" }  eq "" ) {
		LogIt( "FATAL! You must provide a valid MySQL user account." );
   		UsageHint();
       	exit( 1 );
	}

	# Do we have a DBCONFIGFILE?
	if ( ! $CLP{ "DBCONFIGFILE" } || $CLP{ "DBCONFIGFILE" }  eq "" ) {
		LogIt( "FATAL! You must provide a valid DB configuration file." );
		UsageHint();
		exit( 1 );
	}

	# Do we have a FECONFIGFILE?
	if ( ! $CLP{ "FECONFIGFILE" } || $CLP{ "FECONFIGFILE" }  eq "" ) {
		LogIt( "FATAL! You must provide a valid FE configuration file." );
		UsageHint();
 		exit( 1 );
	}

	# Do we have a TESTRUN?
	if ( ! $CLP{ "TESTRUN" } || $CLP{ "TESTRUN" }  eq "" ) {
		# if not specified, use default
		$CLP{ "TESTRUN" } = $DEFAULTS{ "TESTRUN" };
	}

	# Do we have a QUIET?
	if ( ! $CLP{ "QUIET" } || $CLP{ "QUIET" }  eq "" ) {
		# if not specified, use default
		$CLP{ "QUIET" } = $DEFAULTS{ "QUIET" };
	}

	# Do we have a WAIT_TIME?
	if ( ! $CLP{ "WAIT_TIME" } || $CLP{ "WAIT_TIME" }  eq "" ) {
		# if not specified, use default
		$CLP{ "WAIT_TIME" } = $DEFAULTS{ "WAIT_TIME" };
	}

} # Parse the command line

# Load the DB server list.
sub LoadDBServers
{
        my ($DBDataFile) = @_;

        open( my $DBFileHandle, "<", $DBDataFile ) or die "FATAL! Can't open $DBDataFile for reading!\n";
        flock( $DBFileHandle, 2);

        while ( <$DBFileHandle> ) {
                # Ignore comments that may be in the file.
                if ( " #/;" !~ /substr( $_, 0, 1 )/ ) {
                        chomp( $_ );
                        my @mField = split( /:/, $_ );
                        $ServerPort{ $mField[0] } = $mField[1];
                        $ServerMasterUser{ $mField[0] } = $mField[2];
                        $ServerMasterPass{ $mField[0] } = $mField[3];
                        $ServerPermRole{ $mField[0] } = uc( $mField[4] );
                        $ServerCurRole{ $mField[0] } = uc( $mField[5] );
						$ServerOldCurRole{ $mField[0] } = uc( $mField[5] );
						
                        if ( $ServerPermRole{ $mField[0] } eq "B" ) {
                                $BothCount = $BothCount + 1;
                        }

                        # Is this the current master server as we know it?
                        if ( "BM" =~ /$ServerPermRole{ $mField[0] }/  && $ServerCurRole{ $mField[0] } eq "M" ) {
                                $MasterCount = $MasterCount + 1;
                                if ( $OldMaster ne "" ) {
                                        LogIt( "LoadDBServers: FATAL! $DBDataFile is not correct!" );
                                        LogIt( "LoadDBServers: More than one master server is defined as the CURRENT master!" );
                                        LogIt( "LoadDBServers: Only one master may be defined as the CURRENT master." );
                                        LogIt( "LoadDBServers: Check servers $OldMaster and $mField[0] in $DBDataFile." );
                                        UsageHint();
                                        exit( 1 );
                                } else {
                                        $OldMaster =  $mField[0];
                                        LogIt( "LoadDBServers: $OldMaster is currently the master." );
                                }
                        } #valid master?
                        
                        if ( "BS" =~ /$ServerPermRole{ $mField[0] }/ ) {
								if ($ServerCurRole{ $mField[0] } eq "S" ) {
                        			$DBSlaves{ $mField[0] } = $mField[0];
									$SlaveCount = $SlaveCount + 1;
                        			LogIt( "LoadDBServers: $mField[0] is currently a slave." );
								} elsif ($ServerCurRole{ $mField[0] } eq "F" || $ServerCurRole{ $mField[0] } eq "R" ) {
                        			$FailedDBSlaves{ $mField[0] } = $mField[0];
									$SlaveCount = $SlaveCount + 1;
                        			LogIt( "LoadDBServers: $mField[0] is currently a failed slave." );
								}
                       } #valid slave? 
                }
        } # while
        close( $DBFileHandle );
}# Load the DB server list.

# Load the FE server list.
sub LoadFEServers
{
        my ($FEDataFile) = @_;

        open( my $FEFileHandle, "<", $FEDataFile ) or die "FATAL! Can't open $FEDataFile for reading!\n";
        flock( $FEFileHandle, 2);

        my $num = 0;
        while ( <$FEFileHandle> ) {
                # Ignore comments that may be in the file.
                if ( " #/;" !~ /substr( $_, 0, 1 )/ ) {
                        chomp( $_ );
                        my @mField = split( /:/, $_ );
                        $FEServerUser{ $mField[0] } = $mField[1];
                        $FEServerConfigPath{ $mField[0] } = $mField[2];
                        $FEServerStatus{ $mField[0] } = $mField[3];

                        if ( $mField[3] == 1 ) {
                                $num ++;
                                $WebClients{ $mField[1].'@'.$mField[0] } = $mField[2];
                        }
                }
        } # while
        close( $FEFileHandle );
}# Load the FE server list.

# Step 1 - Connect to all the Database servers.
sub ConnectToDBS
{
        my $Server = "";
        my $MyDBIURL = "";
		my $NotDone = 1;
        my $MyDBIOpts = { PrintError => 0 };

        foreach $Server (  keys %ServerCurRole ) {

			if ( "BMS" =~ /$ServerPermRole{ $Server }/ ) {
    	  		if ( $CLP{ "TESTRUN" } == 0 ) {
					$MyDBIURL = "DBI:mysql:mysql:$Server:$ServerPort{ $Server };mysql_connect_timeout=5";
        	  		$ServerConn{ $Server } = DBI->connect( $MyDBIURL, $CLP{ "USER" }, $CLP{ "PASSWORD" }, $MyDBIOpts );

					if ( $ServerCurRole{ $Server } eq "M" ) {
						# master
	   	     			LogIt( "ConnectToDBS: Connecting to MASTER server $Server:$ServerPort{ $Server }" );
						my $tries = 0;
						while ( $NotDone )  {
							
							if ( ! defined( $ServerConn{ $Server } ) ) {
								$tries ++;
								if ( $tries <= $DEFAULTS{ 'MAX_CONNECTION_RETRIES' } ) { 
									 LogIt( "ConnectToDBS: retry connection $Server:$ServerPort{ $Server } pause" );	
									$NotDone = 1;
									sleep( 2 );
									$MyDBIURL = "DBI:mysql:mysql:$Server:$ServerPort{ $Server };mysql_connect_timeout=5";
        	  						$ServerConn{ $Server } = DBI->connect( $MyDBIURL, $CLP{ "USER" }, $CLP{ "PASSWORD" }, $MyDBIOpts );
 								} else {
									$NotDone = 0;
								}	
							} else {
								$NotDone = 0;
							}
						}
					}	

	          		if ( ! defined( $ServerConn{ $Server } ) ) {
    	     			# Connection failed to the server.
        	      		LogIt( "ConnectToDBS: ERROR! Failed to connect to $Server:$ServerPort{ $Server }. The error was:" );
            	    	LogIt( "ConnectToDBS: $DBI::errstr" );
             			$ServerCurRole{ $Server } = "F";
	   	     			LogIt( "Connecting to server $Server:$ServerPort{ $Server } as user $CLP{ 'USER' }. X" );
              		} else {
	   	     			LogIt( "ConnectToDBS: Connecting to server $Server:$ServerPort{ $Server } as user $CLP{ 'USER' }. OK" );

						my %stat = GetServerStatus($Server);
						$ServerStatus{ $Server } = \%stat;
						my %rep = GetReplicationStatus($Server);
						$ReplicationStatus{ $Server } = \%rep;
	
						if ( defined( $ServerStatus{ $Server } ) ) {
							LogIt( "ConnectToDBS: SERVER=$Server, CONNECTION=$ServerConn{ $Server }, ROLE=$ServerCurRole{ $Server }, ProcessLogs=" . HasProcessedRelayLogs(  $Server ) . ", THREADS=$ServerStatus{ $Server }{ 'Threads_connected' }, UPTIME=$ServerStatus{ $Server }{ 'Uptime' }" );
						}

						if ( defined( $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } ) ) { 
							LogIt( "ConnectToDBS: MASTER_HOST=$ReplicationStatus{ $Server }{ 'Master_Host'}, SECONDS_BEHIND=$ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' }" ); 
						}
              		}
    	  		} # TESTRUN?
   			}
   			
        } # foreach $Server

        LogIt( "STEP 1: Connecting to servers: Done." );
} # Step 1 - Connect to all the Database servers.

sub DisconnectFromDBS
{
	foreach my $Server ( keys %ServerPermRole ) {
		if ( $CLP{ "TESTRUN" } == 0 ) {
			# Close MySQL Handle/Connection
			if ( defined( $ServerConn{ $Server } ) ) {
				$ServerConn{ $Server }->disconnect();
				LogIt( "DisconnectFromDBS: Disconnectioning from server $Server." );
			}
		} # TESTRUN
	}# Server
} # DisconnectFromDBS

# Step 2 - Check for any failed slaves
# if cannot connect to slave remove from drupal front end web clients
sub CheckFailedSlaves
{
	my (%Slaves) = @_;
	my $Failed = 0;
	my %FailedSlaves;
	my $Msg;

	foreach my $Server ( keys %Slaves ) {
		$Msg = "CheckFailedSlaves: Checking connection to slave $Server. ";
		# if server is down, update frontend
		if ( ! defined( $ServerConn{ $Server } ) ) {
			$ServerCurRole{ $Server } = "F";
			$FailedSlaves{ $Server } = $Server;
			$Failed = 1;
			$Msg .= "X";
			AlertIt( "$Server", "ALERT_SLAVE_F", "ALERT MySQL Slave $Server Failed", "MySQL_check_replication.pl has changed the status of MySQL slave $Server to F.");
		} else {
			$Msg .= "OK";
		}
		LogIt($Msg);
	}

	if ($Failed == 1 && $CLP{ "TESTRUN" } == 0) {
		%FailedDBSlaves = %FailedSlaves;
		
		# Write out new db dat file with new configuration.
		WriteConfig($TmpDBDataFile);

		# Distribute drupal db config file
		DistributeConfig($TmpDBDataFile);		

	}

 	LogIt( "STEP 2: Check for any failed slaves: Done." );
}# Step 2 - Check for any failed slave(s)


# Step 3 - Check for any slave(s) that have comeback online
# if slave come back online add from drupal front end web clients
sub CheckBackOnlineSlaves
{
	my (%Slaves) = @_;
	my $Online = 0;
	my %OnlineSlaves;
	my %Msg;
	my %Subject;
	my %AlertType;
	my %RunCmd;
	my %UpdateWebClients;
	my $exec_result;

	foreach my $Server ( keys %Slaves ) {

		# if slave server is backup
		if ( defined( $ServerConn{ $Server } ) ) {
			
			# if status is (F)ailed
			if  ( $ServerOldCurRole{ $Server } eq "F" 
				&& defined( $ServerStatus{ $Server } ) ) {
				
				my $tmp =  "perl " .dirname (__FILE__) ."/MySQL_reset_slave.pl --master=$CLP{ 'SERVER' } --server=$Server --user=$CLP{ 'USER' }";
				if (defined ($CLP{ 'PASSWORD' }) && $CLP{ 'PASSWORD' } ne "") { $tmp .= " --password=$CLP{ 'PASSWORD' }"; }
				$tmp .= " --dbconfigfile=$CLP{ 'DBCONFIGFILE' } --feconfigfile=$CLP{ 'FECONFIGFILE' }";
				if ($CLP{ "TESTRUN" } != 0) { $tmp .= " --test"; }
				$RunCmd{ $Server } = $tmp;
				$UpdateWebClients{ $Server } = $Server;
				$ServerCurRole{ $Server } = "R";
				LogIt( "CheckBackOnlineSlaves: Resetting slave server $Server" );
				$AlertType{ $Server } = "ALERT_SLAVE_R";
				$Subject{ $Server } = "ALERT MySQL Slave $Server is recovering";
				$Msg{ $Server } = "MySQL_check_replication.pl has changed the status of MySQL slave $Server to R"; 
					
			} else {

				my %stat = GetServerStatus($Server);
     			$ServerStatus{ $Server } = \%stat;

				my %rep = GetReplicationStatus($Server);
       			$ReplicationStatus{ $Server } = \%rep;
			
				# check if R slave has caught up
				if ( defined( $ServerStatus{ $Server } )
					&& defined( $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } )
					&& $ServerCurRole{ $Server } eq "R"
					&& $ServerStatus{ $Server }{ 'Threads_connected' } < $DEFAULTS{ 'MAX_CONNECTIONS' }
					&& $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } == 0 ) {
						
						$ServerCurRole{ $Server } = "S";	
						$UpdateWebClients{ $Server } = $Server;
						LogIt( "CheckBackOnlineSlaves: Updating server $Server to slave" );
						$AlertType{ $Server } = "ALERT_SLAVE_S";
						$Subject{ $Server } = "ALERT MySQL Slave $Server is back online";
						$Msg{ $Server } = "MySQL_check_replication.pl has changed the status of MySQL slave $Server to S.";
				}
			}
		} else {
			# ensure that (R)ecovering slaves are made (F)ail if down
			if ($ServerOldCurRole{ $Server } ne "F") {
          		$ServerCurRole{ $Server } = "F";
        		$UpdateWebClients{ $Server } = $Server;
             	$AlertType{ $Server } = "ALERT_SLAVE_F";
           		$Subject{ $Server } = "ALERT MySQL Slave $Server Failed";
           		$Msg{ $Server } = "MySQL_check_replication.pl has changed the status of MySQL slave $Server to F."
			}
		} # ServerConn	
	}
	
	my $NumCmds =  scalar keys %RunCmd;
	my $NumClients = scalar keys %UpdateWebClients;

	# update webclients
	#my $NumClients = scalar keys %UpdateWebClients;
	if ( $NumClients > 0 &&  $CLP{ "TESTRUN" } == 0 ) {
					
		# Write out new db dat file with new configuration.
		WriteConfig($TmpDBDataFile);

		# Distribute drupal db config file
		DistributeConfig($TmpDBDataFile);
				
		foreach my $client ( keys %UpdateWebClients ) {
			if ( defined( $AlertType{ $client } ) && defined( $Subject{ $client } )  && defined( $Msg{ $client } ) ) {
				AlertIt( "$client", $AlertType{ $client }, $Subject{ $client }, $Msg{ $client } );	
			}
		}
	}

	# execute cmds
	if ( $NumCmds > 0 && $CLP{ "TESTRUN" } == 0 ) {
		foreach my $cmd ( keys %RunCmd ) {
			$exec_result = `$RunCmd{ $cmd } 2>&1`;
			LogIt( "CheckBackOnlineSlaves: Executing $RunCmd{ $cmd }" );
			if ($? != 0) {
				LogIt( "CheckBackOnlineSlaves: Failed to reset slave $cmd." );
			}
			LogIt( "CheckBackOnlineSlaves:: $exec_result");
		}
	}
	
	LogIt(  "STEP 3 Check for any slaves that have come back online: Done.");
} # Step 3 - Check for any slaves that have come back online


# Get the number of thread connections
sub GetConnectionStatus
{
	my ($Server) = @_;
	my $mSQL ="SHOW STATUS LIKE 'Threads_connected';";
	my $mQuery;
	my $num = 0;
	if ( $CLP{ "TESTRUN" } == 0 ) {
		$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
		$mQuery->execute();
		my $num = $mQuery->fetchrow_array();
		LogIt( "GetConnectionStatus: Server $Server has $num connections." );
	} # TESTRUN
	return $num;
}

# Get server status for given server
sub GetServerStatus
{
	my ($Server) = @_;
	my $mSQL;
	my $mQuery;
	my %status;
	if ( "BMS" =~ /$ServerPermRole{ $Server }/  ) {
		$mSQL = "SHOW STATUS;";
		if ( $CLP{ "TESTRUN" } == 0 ) {	
			$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
			$mQuery->execute();
			while (my $mHash = $mQuery->fetchrow_hashref()) {
				my $name = $mHash->{ 'Variable_name' };
				my $value  = $mHash->{ 'Value' };
				$status{ $name } = $value;
			} 
		}
	} # BMS
	LogIt( "GetServerStatus: Getting the sever status for server $Server: Done." );
	return %status;
}

# For a given server; Closes all open tables and locks all tables for all databases with a global read lock
sub LockTables
{
	my ($Server) = @_;

	my $mSQL="FLUSH TABLES WITH READ LOCK;";
	my $mQuery;
	my $status=0;
	if ( defined( $ServerConn{ $Server } ) ) {
		if ( $CLP{ "TESTRUN" } == 0 ) {
			$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
			my $rv = $mQuery->execute();
			if ($rv) {
				$status=1;
			} else {
				LogIt ("LockTables: Locking $Server: Failed.");
			}
		}# TESTRUN
	}# ServerConn

	LogIt ("LockTables: Locking $Server: Done.");
	return $status;	
	
}# LockTables

# For a given server; releases any table locks
sub UnlockTables
{
	my ($Server) = @_;
	my $mSQL="UNLOCK TABLES;";
	my $mQuery;
	my $status=0;
	if ( defined( $ServerConn{ $Server } ) ) {
		if ( $CLP{ "TESTRUN" } == 0 ) {
			$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
			my $rv = $mQuery->execute();
			if ($rv) {
				$status=1;
			} else {
				LogIt ("UnlockTables: UnLocking $Server: Failed");
			}
		}# TESTRUN
	}# ServerConn

	LogIt ("UnlockTables: UnLocking $Server: Done");
	return $status;

}# UnlockTables	


# Get replication status for given server
sub GetReplicationStatus
{
	my ($Server) = @_;
	my $mSQL;
	my $mQuery;
	my %status=();
	if ( "BMS" =~ /$ServerPermRole{ $Server }/  && defined( $ServerConn{ $Server } ) ) {
		if ( $ServerCurRole{ $Server } eq "M" ) {
			$mSQL = "SHOW MASTER STATUS;";
		} # Master?
			
		if ( $ServerCurRole{ $Server } eq "S" || $ServerCurRole{ $Server } eq "X"  || $ServerCurRole{ $Server } eq "R" ) {
			$mSQL = "SHOW SLAVE STATUS;";
		} # Slave?
		
		if ( defined( $mSQL ) && $CLP{ "TESTRUN" } == 0 ) {	
			$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
			$mQuery->execute();
			my $result = $mQuery->fetchrow_hashref();
			foreach my $name (keys %$result) {	
				$status{ $name } = $result->{$name};
			} 
		}
	} # BMS
	LogIt( "GetReplicationStatus: Getting the replication status for server $Server: Done." );

	return %status;
}

# determine whether the given slave server has processed its relay logs
# returns 1 true, 0 false
sub HasProcessedRelayLogs 
{
	my ($Server) = @_;
	my $mSQL = "SHOW PROCESSLIST;";
	my $mQuery;
	my $ProcessedLogs = 0;
	if ( "BMS" =~ /$ServerPermRole{ $Server }/  && ($ServerCurRole{ $Server } eq "S" || $ServerCurRole{ $Server } eq "R") ) {
		# Prepare and Execute the Query Statement
		if ( $CLP{ "TESTRUN" } == 0 ) {
			$mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
			$mQuery->execute();
			my @mRow;
			while ( @mRow = $mQuery->fetchrow() ) {
				if ( $mRow[ 1 ] eq "system user" || $mRow[ 2 ] eq "system user" ) {
					if ( uc( $mRow[ 6 ] ) =~ /HAS READ ALL RELAY LOG/  ||  uc( $mRow[ 7 ] ) =~ /HAS READ ALL RELAY LOG/) {
						# this slave server has processed its relay logs.";
						return 1;
					}
				}
			}
		}
	}
	return 0;
}

# check replication status for each server
sub CheckReplicationStatus
{
   	foreach my $Server ( keys %ServerCurRole ) {
		if ($ServerCurRole{ $Server } eq "F") { next; }
		my $mSQL = "";
		
		my %stat = GetServerStatus($Server);
     	$ServerStatus{ $Server } = \%stat;

		my %rep = GetReplicationStatus($Server);
       	$ReplicationStatus{ $Server } = \%rep;

		if ( "BMS" =~ /$ServerPermRole{ $Server }/  ) {

			if ( defined( $ReplicationStatus{ $Server } ) && $ServerCurRole{ $Server } eq "S" ) {
				# If no replication io or sql thread
				if ( defined( $ReplicationStatus{ $Server }{ 'Slave_IO_Running'} )
					&& defined( $ReplicationStatus{ $Server }{ 'Slave_SQL_Running'} ) 
					&& $ReplicationStatus{ $Server }{ 'Slave_IO_Running'} ne "Yes" 
					|| $ReplicationStatus{ $Server }{ 'Slave_SQL_Running'} ne "Yes") {
					LogIt( "CheckReplicationStatus: Replication has failed for server $Server." );
					AlertIt( "$Server", "ALERT_REPLICATION_FAIL", "ALERT MySQL Replication Failed", "MySQL_check_replication.pl has detected that replication for $Server has failed.");
				}
				
				# If slave has replication lag set status to R
				if ( defined( $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } ) 
					&& $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } > $DEFAULTS{ 'MIN_SECONDS_BEHIND_MASTER' } ) {
					$ServerCurRole{ $Server } = "R";
					LogIt( "CheckReplicationStatus: Replication lag for server $Server." );
					AlertIt( "$Server", "ALERT_SLAVE_R", "ALERT MySQL Replication Lag", "MySQL_check_replication.pl has detected $Server is $ReplicationStatus{ $Server }{ 'Seconds_Behind_Master' } seconds behind master set status to R.");
					
					# Write out new db dat file with new configuration.
					WriteConfig($TmpDBDataFile);
		
					# Distribute drupal db config file
					DistributeConfig($TmpDBDataFile);
				}

				if ( defined( $ServerConn{ $OldMaster } )
					&& $ReplicationStatus{ $Server }{ 'Master_Host' }
					&& $ReplicationStatus{ $Server }{ 'Master_Host' } ne $OldMaster ) {
					LogIt( "CheckReplicationStatus: Replication Master_Host is not connected to master $OldMaster for server $Server." );
					AlertIt( "$Server", "ALERT_REPLICATION_FAIL", "ALERT MySQL Replication Master Host", "MySQL_check_replication.pl has detected that slave $Server has the wrong master host.");
				}				
			}
		} # BMS
	} # Server

}## check replication status for each server


# Step 4 - Check for failed master
# if cannot connect to current master promote a slave as master and update drupal front end web clients
sub CheckFailedMaster
{
	my ($Master, %Slaves) = @_;
	my $Failed = 0;
	my ($PromotedSlave, $Cmd, $CmdStatus, $CmdResult);

	if ( ! defined( $ServerConn{ $Master } ) ) {
		# Master is down
		LogIt( "CheckFailedMaster: Checking connection to master $Master. X" );

		## 1st, remove failed master from drupal
		$ServerCurRole{ $Master } = "F";

		# Write out new db dat file with new configuration.
		WriteConfig($TmpDBDataFile);
		
		# Distribute drupal db config file
		DistributeConfig($TmpDBDataFile);

		# 2nd, try to promote primary master if available
		if ( defined( $ServerConn{ $CLP{ 'SERVER' } } ) 
			&& $ServerCurRole{ $CLP{ 'SERVER' } } eq "S" || $ServerCurRole{ $CLP{ 'SERVER' } } eq "R" 
			&& defined( $ServerStatus{ $CLP{ 'SERVER' } }{ 'Threads_connected' } )
			&& defined( $ReplicationStatus{ $CLP{ 'SERVER' } }{ 'Seconds_Behind_Master' } )
			&& $ServerStatus{ $CLP{ 'SERVER' } }{ 'Threads_connected' } < $DEFAULTS{ 'MAX_CONNECTIONS' } 
			&& $ReplicationStatus{ $CLP{ 'SERVER' } }{ 'Seconds_Behind_Master' } == 0
			&& HasProcessedRelayLogs(  $CLP{ 'SERVER' } ) ) {
			$PromotedSlave = $CLP{ 'SERVER' };

			# build promote command
			$Cmd=GetPromoteCmd( $PromotedSlave );

			($CmdStatus, $CmdResult) = ExecuteCmd( $Cmd );		
			if ($CmdStatus == 1) {
				LogIt( "CheckFailedMaster: Successfully promoted slave $PromotedSlave to master." );
				$ServerCurRole{ $PromotedSlave } = "M";
				AlertIt( "$PromotedSlave", "ALERT_MASTER_F", "ALERT MySQL Master $Master Failed", "MySQL_check_replication.pl has detected that MySQL master $Master had failed and thus promoted slave $PromotedSlave to master.");
			} else {
				LogIt( "CheckFailedMaster: Failed to promote slave $PromotedSlave to master." );
				LogIt( "CheckFailedMaster: $CmdResult" );
			}	
		} else {
			# if cannot promote primary master, promote one of the other slaves
			foreach $PromotedSlave ( keys %Slaves ) {
                # build promote command
				$Cmd=GetPromoteCmd( $PromotedSlave );

				($CmdStatus, $CmdResult) = ExecuteCmd( $Cmd );
				if ($CmdStatus == 1) {
					LogIt( "CheckFailedMaster: Successfully promoted slave $PromotedSlave to master." );
					$ServerCurRole{ $PromotedSlave } = "M";
					AlertIt( "$PromotedSlave", "ALERT_MASTER_F", "ALERT MySQL Master $Master Failed", "MySQL_check_replication.pl has detected that MySQL master $Master had failed and thus promoted slave $PromotedSlave to master.");
					last;
				} else {
					LogIt( "CheckFailedMaster: Failed to promote slave $PromotedSlave to master." );
					LogIt( "CheckFailedMaster: $CmdResult" );
				}
			}
		}
					
	} else {
		LogIt( "CheckFailedMaster: Checking connection to master $Master. OK" );
		LogIt( "Master $Master is OK nothing to do." );
	}	
	LogIt( "STEP 4: Check for failed master: Done." );	
}# Step 4 - Check for failed master


# Step 5 - Check for master that has comeback online
# if slave come back online add from drupal front end web clients
sub CheckBackOnlineMaster
{
	my ($Master, %Slaves) = @_;
	my $Online = 0;
	my ($PromotedSlave, $RunCmd, %Msg, %AlertType, $LockMaster);

    my %stat = GetServerStatus($CLP{ 'SERVER' });
    $ServerStatus{ $CLP{ 'SERVER' } } = \%stat;

    my %rep = GetReplicationStatus($CLP{ 'SERVER' });
    $ReplicationStatus{ $CLP{ 'SERVER' } } = \%rep;

	# firt, if primary master comeback online then, make it a slave
	if ( defined( $ServerConn{ $Master } )  
		&& defined( $ServerConn{ $CLP{ 'SERVER' } } )
		&& $Master ne $CLP{ 'SERVER' }
		&& $ServerOldCurRole{ $CLP{ 'SERVER' } } eq "F"
		&& defined ( $ServerStatus{ $CLP{ 'SERVER' } }{ 'Threads_connected' } ) ) {
		$ServerCurRole{ $CLP{ 'SERVER' } } = "R";
		$PromotedSlave =  $CLP{ 'SERVER' };
		$AlertType{ $PromotedSlave } = "ALERT_MASTER_R";
		$Msg{ $PromotedSlave } = "MySQL_check_replication.pl changed the status of the primary MySQL master $CLP{ 'SERVER' } to R.";

		# Write out new db dat file with new configuration.
		WriteConfig($TmpDBDataFile);

		# Distribute drupal db config file
		DistributeConfig($TmpDBDataFile);
	
		# build command
		$RunCmd = "perl " .dirname (__FILE__) ."/MySQL_reset_slave.pl --master=$CLP{ 'SERVER' } --server=$PromotedSlave --user=$CLP{ 'USER' }";
		if (defined ($CLP{ 'PASSWORD' }) && $CLP{ 'PASSWORD' } ne "") { $RunCmd .= " --password=$CLP{ 'PASSWORD' }"; }
        $RunCmd .= " --dbconfigfile=$CLP{ 'DBCONFIGFILE' } --feconfigfile=$CLP{ 'FECONFIGFILE' }";
        if ($CLP{ "TESTRUN" } != 0) { $RunCmd .= " --test"; }

	} elsif ( defined( $ServerConn{ $CLP{ 'SERVER' } } ) 
		&& $ServerCurRole{ $CLP{ 'SERVER' } } eq "R" || $ServerCurRole{ $CLP{ 'SERVER' } } eq "S" 
		&& defined( $ServerStatus{ $CLP{ 'SERVER' } }{ 'Threads_connected' } )
		&& defined( $ReplicationStatus{ $CLP{ 'SERVER' } }{ 'Seconds_Behind_Master' } )
		&& $ServerStatus{ $CLP{ 'SERVER' } }{ 'Threads_connected' } < $DEFAULTS{ 'MAX_CONNECTIONS' } 
		&& $ReplicationStatus{ $CLP{ 'SERVER' } }{ 'Seconds_Behind_Master' } == 0
		&& HasProcessedRelayLogs(  $CLP{ 'SERVER' } ) ) {

		# hack to stop writes to master during re-promotion
		my $OldRole = $ServerCurRole{ $Master };
		$ServerCurRole{ $Master } = "S";

		WriteConfig($TmpDBDataFile);
		DistributeConfig($TmpDBDataFile);

		$ServerCurRole{ $Master } = $OldRole;
		
		# pause for a while to allow server catchup
		sleep 10;

		$PromotedSlave = $CLP{ 'SERVER' };
		$AlertType{ $PromotedSlave } = "ALERT_MASTER_M";
		$Msg{ $PromotedSlave } = "MySQL_check_replication.pl has changed the status of the primary MySQL master $CLP{ 'SERVER' } to M";
					
		# build command line
		$RunCmd = "perl " .dirname (__FILE__) ."/MySQL_promote_slave.pl --server=$PromotedSlave --user=$CLP{ 'USER' }";
		if (defined ($CLP{ 'PASSWORD' }) && $CLP{ 'PASSWORD' } ne "") { $RunCmd .= " --password=$CLP{ 'PASSWORD' }"; }
		$RunCmd .= " --dbconfigfile=$CLP{ 'DBCONFIGFILE' } --feconfigfile=$CLP{ 'FECONFIGFILE' }";
		if ($CLP{ "TESTRUN" } != 0) { $RunCmd .= " --test";}
	}			

	if ($RunCmd && $RunCmd ne "") {
		LogIt( "CheckBackOnlineMaster: executing: $RunCmd");
		if ( $CLP{ "TESTRUN" } == 0 ) {
			my $exec_result = `$RunCmd 2>&1`;
			if ($? == 0) {
				LogIt( "CheckBackOnlineMaster: " . $Msg{ $PromotedSlave } );
				AlertIt( "$PromotedSlave", $AlertType{ $PromotedSlave }, "ALERT MySQL Master $PromotedSlave", $Msg{ $PromotedSlave } );
				
				# Unlock tables
				#if  ( defined( $LockMaster ) && $LockMaster == 1 ) {
				#	my $unlocked = UnlockTables( $Master );
				#	while ( $unlocked == 0 ) {
				#		$unlocked = UnlockTables( $Master );
				#	}
				#}

			} else {
				LogIt( "CheckBackOnlineMaster: Failed to execute $RunCmd." );
				LogIt( "CheckBackOnlineMaster: $exec_result");
    		}
		} # TESTRUN
	} else {
		LogIt( "CheckBackOnlineMaster: nothing to do." );
	}

	LogIt( "STEP 5: Check for online master: Done." );	
}# Step 5 - Check for online master


# Validation
sub Validate
{

	# Make sure the Primary Master is in the list. If not, then stop.
	if ( ! $ServerCurRole{ $CLP{ "SERVER" } } ) {
		LogIt( "FATAL! $CLP{ 'SERVER' } not found in the $CLP{ 'DBCONFIGFILE' } file." );
		UsageHint();
		exit( 1 );
	}


	# Check to make sure Priamry Master machine is permitted to be a MASTER server.
	if ( "BM" !~ /$ServerPermRole{ $CLP{ "SERVER" } }/ ) {
		LogIt( "FATAL! Server $CLP{ 'SERVER' } is not configured to be a MASTER server." );
		LogIt( "The server must have the permissible role as either M)aster or B)oth." );
		UsageHint();
		exit( 1 );
	}

}# Validate

# Write out new db dat file with new configuration.
sub WriteConfig
{
        my ($TmpFile) = @_;

        $ErrorCount = 0;
        $SlaveCount = 0;

        if ( $CLP{ "TESTRUN" } == 0 ) {
         	LogIt( "Updating $TmpDBDataFile with new role information." );
        	open( TmpDBFileHandle, ">", $TmpFile ) or die "FATAL! Can't open the $TmpFile for writing!\n";
        	flock( TmpDBFileHandle, 2);
        } else {
          	LogIt( " " );
       		LogIt( "$TmpFile file will look like this:" );
        } # $CLP{ "TESTRUN" }

		foreach my $Server ( keys  %ServerPermRole ) {
     		if ( $CLP{ "TESTRUN" } == 0 ) {
                # update the tmp db .dat file.
                print( TmpDBFileHandle "$Server:$ServerPort{ $Server }:$ServerMasterUser{ $Server }:$ServerMasterPass{ $Server }:$ServerPermRole{ $Server }:$ServerCurRole{ $Server }\n" );
        	} else {
                LogIt( "$Server:$ServerPort{ $Server }:$ServerMasterUser{ $Server }:$ServerMasterPass{ $Server }:$ServerPermRole{ $Server }:$ServerCurRole{ $Server }" );
        	}

        	# Is this a slave server?
        	if ( $ServerCurRole{ $Server } eq "S" )    {
                $SlaveCount = $SlaveCount + 1;
        	}

        	# Did we have problems with this server?
        	if ( $ServerCurRole{ $Server } eq "F" )    {
                #$ErrorCount = $ErrorCount + 1;
                LogIt( "WARNING! $Server had a problem and was not successfully modified." );
        	}
        } # foreach $SlaveServer

        if ( $CLP{ "TESTRUN" } == 0 ) {
       		close( TmpDBFileHandle );
        	if (-f $TmpFile ) { $ConfigCreated = 1; }
        } else {
        	LogIt( " " );
        } # $CLP{ "TESTRUN" }

}# Write out new db dat file with new configuration.

# Distribute drupal db config file
sub DistributeConfig
{
        my ($TmpFile) = @_;
        my $status = 1;
        if ( $CLP{ "TESTRUN" } == 0 && $ConfigCreated == 1 && $ErrorCount == 0) {
        while (my ($login, $config_path) = each %WebClients) {
        	#-- scp file to web clients and cature STDERR as well as STDOUT
          	my $config_file = $config_path . basename($CLP{ 'DBCONFIGFILE' });
          	my $exe_cmd = "scp $TmpFile $login:$config_file";
          	my $exec_result = `$exe_cmd 2>&1`;
          	if ($? == 0) {
            	LogIt( "DistributeConfig: Successfully updated $login $config_file." );
         	} else {
           		$status = 0;
                LogIt( "WARNING! Failed to update $login $config_file" );
         	}
      	}
  	}

  	LogIt( "DistributeConfig: Distribute database config file $TmpFile: Done." );
	return $status;
}# Distribute drupal db config file

# execute command
sub ExecuteCmd
{
       my ($Command) = @_;
       my $Result="";
       my $Status = 0;
       LogIt( "Preparing to executing command: $Command" );
       if ( !defined( $Command ) || $Command eq "" || $CLP{ "TESTRUN" } == 0 ) {
               $Result = `$Command 2>&1`;
               if ($? == 0) {
                       $Status = 1;
               } else {
                       LogIt( "Failed to execute command: $Command" );
               }
       } #TESTRUN

       return ($Status, $Result);
}# ExecuteCmd

sub GetPromoteCmd
{
	my ($Server) = @_;
	my $Cmd;
	
	$Cmd = "perl " .dirname (__FILE__) ."/MySQL_promote_slave.pl --server=$Server --user=$CLP{ 'USER' }";
	if (defined ($CLP{ 'PASSWORD' }) && $CLP{ 'PASSWORD' } ne "") { $Cmd .= " --password=$CLP{ 'PASSWORD' }"; }
	$Cmd .= " --dbconfigfile=$CLP{ 'DBCONFIGFILE' } --feconfigfile=$CLP{ 'FECONFIGFILE' }";
	if ($CLP{ "TESTRUN" } != 0) { $Cmd .= " --test"; }

	return $Cmd;
} # GetPromoteCmd



#
# Subroutines
##############################################################################

# (end of file)

