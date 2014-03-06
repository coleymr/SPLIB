#!/usr/bin/perl
# MySQL_promote_slave.pl
# Version 1.1.1
# Last Modified 2012-01-29
# Originally written by: Van Stokes Jr
#
# This script will promote a properly configured MySQL slave to a MySQL master
# while demoting the current MASTER (if its up). It will also reconfigure other
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
#  6) This does NOT monitor servers or determine their state.
#
# REQUIRED TEXT FILE: drupal_db_servers.dat
# There must be at lease two MySQL servers in this file.
# There must be at least: one (PR:B) as master (CR:M) and; one (PR:B) as a slave (CR:S).
# To promote you must have at least TWO servers capable of being masters.
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
#	client01:root:/var/www/html/sites/default/:0
#	client02:root:/var/www/html/sites/default/:1
#	client03:root:/var/www/html/sites/default/:1

# Example execution:
# For testing:
#    ./MySQL_promote_slave.pl --server=dbserver --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --test
# For real:
#    ./MySQL_promote_slave.pl --server=dbserver --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --test
#
#

use strict;
use warnings;
use DBI;
use File::Basename;
use Cwd;
use POSIX qw(tmpnam);
use Data::Dumper;

# globals
my %WebClients; 
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
my $ConfigCreated = 0;
my $CopyFileCount = 0;
my $ErrorCount = 0;
my $BothCount = 0;
my $MasterCount = 0;
my $SlaveCount = 0;
my $OldMaster = "";
my %CLP;
my $TmpDBDataFile = tmpnam();
$CLP{ "QUIET" } = 0;
$CLP{ "TESTRUN" }  = 0;

# Parse the command line arguments
ParseCommandLine();

# Load the DB server list.
LoadDBServers ($CLP{ 'DBCONFIGFILE' });

# Load the FE server list.
LoadFEServers ($CLP{ 'FECONFIGFILE' });

# Validation
Validate();

# Step 1 - Connect to all the Database servers.
ConnectToDBS();

# Step 2 - Stop all replication services.
StopReplication();

# Step 3 - Make sure that all slaves have processed all statements in their relay log
ProcessRelayLogs();

# Step 4 - Get the replication status for each server.
GetReplicationStatus();

# Step 5 - On the slave being promoted to master,
# issue STOP SLAVE and RESET MASTER.
ConfigureNewMaster($CLP{ 'SERVER' });

# Step 6 - On the other slaves use STOP SLAVE and
# CHANGE MASTER TO MASTER_HOST='NewMaster'
ConfigureSlaves($CLP{ 'SERVER' });

# Write out new db dat file with new configuration.
WriteConfig($TmpDBDataFile);

# Distribute drupal db config file
DistributeConfig($TmpDBDataFile);

# Close MySQL Handle/Connection
if ( defined( $ServerConn{ $CLP{ 'SERVER' } } ) ) {
        $ServerConn{ $CLP{ 'SERVER' } }->disconnect();
}

LogIt( "Successfully promoted $CLP{ 'SERVER' } to master server." );
LogIt( "$SlaveCount slave(s) successfully updated with the new master server." );
LogIt( "$ErrorCount slave(s) NOT updated because a problem ocurred." );

if ( $CLP{ "TESTRUN" } ) {
	LogIt( "     ***** TEST RUN, NOTHING WAS MODIFIED *****" );
}


exit( 0 );

##############################################################################
# Subroutines
#

# Show the programs usage.
sub Usage
{
	print( "\n" );
	print( "Usage: MySQL_promote_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat\n" );
	print( " --server=, -s=        (r) the server to promote to master.\n" );
	print( " --user=, -u=          (r) a MySQL user with privileges to make rep changes.\n" );
	print( " --password=, -p=      (r) the password for --user.\n" );
    print( " --dbconfigfile=, -d=  (r) the drupal database config file.\n" );
    print( " --feconfigfile=, -f=  (r) the drupal database config file.\n" );
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


# Parse the command line arguments
# perl MySQL_promote_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --test
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
                
				if ( uc( $mField[0] ) eq "--QUIET" || uc( $mField[0] ) eq "-Q" ) {
                        $CLP{ "QUIET" } = 1;
                }

        		if ( uc( $mField[0] ) eq "--TEST" || uc( $mField[0] ) eq "-T" ) {
                        $CLP{ "TESTRUN" } = 1;
                }
        }

	# Do we have a USER?
	if ( ! $CLP{ "USER" } || $CLP{ "USER" }  eq "" ) {
		LogIt( "FATAL! You must provide a valid MySQL user account." );
		UsageHint();
		exit( 1 );
	}
	
	# Do we have a PASSWORD?
	#if ( ! $CLP{ "PASSWORD" } || $CLP{ "PASSWORD" }  eq "" )
	#{
	#	LogIt( "FATAL! You must provide the MySQL user account password." );
	#	UsageHint();
	#	exit( 1 );
	#}
	
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
                                        LogIt( "FATAL! $DBDataFile is not correct!" );
                                        LogIt( "More than one master server is defined as the CURRENT master!" );
                                        LogIt( "Only one master may be defined as the CURRENT master." );
                                        LogIt( "Check servers $OldMaster and $mField[0] in $DBDataFile." );
                                        UsageHint();
                                        exit( 1 );
                                } else {
                                        $OldMaster =  $mField[0];
                                        LogIt( "$OldMaster is currently the master." );
                                }
                        } #valid master?

                        # Is this a slave?
                        if ( "BS" =~ /$ServerPermRole{ $mField[0] }/  && $ServerCurRole{ $mField[0] } eq "S" || $ServerCurRole{ $mField[0] } eq "R" || $ServerCurRole{ $mField[0] } eq "X" ) {
                                $SlaveCount = $SlaveCount + 1;
                                $DBSlaves{ $mField[0] } = $mField[0];
                                LogIt( "$mField[0] is currently a slave." );
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

# Validation
sub Validate
{
	# Make sure we have sufficient servers configured in the dat file.
  	if  ( $BothCount < 2 || ($BothCount < 1 && $SlaveCount < 1) ) {
 		LogIt( "FATAL! Insufficient servers configured in $CLP{ 'DBCONFIGFILE' }." );
    	LogIt( "There must be at least: one (PR:B) as master and one (PR:B) as a slave." );
    	UsageHint();
    	exit( 1 );
	}

 	# Make sure we have slave servers in dat file.
	if (scalar(keys %DBSlaves) < 1) {
  		LogIt( "FATAL! No Slave servers configured in $CLP{ 'DBCONFIGFILE' }." );
     	LogIt( "There must be at least: one (PR:B) as a slave." );
    	UsageHint();
    	exit( 1 );
  	}

	# Do we have a SERVER?
	if ( ! $CLP{ "SERVER" } || $CLP{ "SERVER" }  eq "" ) {
		# if not specified SERVER, use found master from db dat file
		$CLP{ "SERVER" } = (keys %DBSlaves)[0];		
		LogIt( "WARNING! You did not provide a valid MySQL server that is configured in $CLP{ 'DBCONFIGFILE' }, setting this to $CLP{ 'SERVER' }." );
	}

	# Make sure the NewMaster is in the list. If not, then stop.
	if ( ! $ServerCurRole{ $CLP{ "SERVER" } } ) {
		LogIt( "FATAL! $CLP{ 'SERVER' } not found in the $CLP{ 'DBCONFIGFILE' }." );
		UsageHint();
		exit( 1 );
	}

	# Check to make New Master machine is permitted to be a MASTER server.
	if ( "BM" !~ /$ServerPermRole{ $CLP{ "SERVER" } }/ ) {
		LogIt( "FATAL! Server $CLP{ 'SERVER' } is not configured to be a MASTER server." );
		LogIt( "The server must have the permissible role as either M)aster or B)oth." );
		UsageHint();
		exit( 1 );
	}

	# Is NewMaster the current master?
	if ( $ServerCurRole{ $CLP{ "SERVER" } } eq "M" ) {
		#LogIt( "STOPPED! Server $CLP{ 'SERVER' } is already the master. Do Nothing!" );
		#exit( 1 );
	}

} # Validation


# Step 1 - Connect to all the Database servers.
sub ConnectToDBS
{
	my $Server = "";
	my $MyDBIURL = "";
	my $MyDBIOpts = { PrintError => 0 };
	foreach $Server (  keys %ServerCurRole ) {
		if ( "BMS" =~ /$ServerPermRole{ $CLP{ "SERVER" } }/ && $ServerCurRole{ $Server } ne "F" ) {
			LogIt( "Connecting to server $Server:$ServerPort{ $Server } as user $CLP{ 'USER' }." );
			if ( $CLP{ "TESTRUN" } == 0 ) {
				$MyDBIURL = "DBI:mysql:mysql:$Server:$ServerPort{ $Server };mysql_connect_timeout=5";
				$ServerConn{ $Server } = DBI->connect( $MyDBIURL, $CLP{ "USER" }, $CLP{ "PASSWORD" }, $MyDBIOpts );

				if ( ! defined( $ServerConn{ $Server } ) ) {
					# Connection failed to the server.
					LogIt( "ERROR! Failed to connect to $Server:$ServerPort{ $Server }. The error was:" );
					LogIt( "$DBI::errstr" );
					$ServerCurRole{ $Server } = "F";
				}
			} # TESTRUN?
		}
	} # foreach $Server

	# Make sure we have a connection to the request new master!
	if ( $CLP{ "TESTRUN" } == 0 && $ServerCurRole{ $CLP{ "SERVER" } } eq "F" ) {
		LogIt( "FATAL! Not connected to the requested new master $CLP{ 'SERVER' }." );
		exit( 1 );
	}
	
	LogIt( "STEP 1: Connecting to servers: Done." );
} # Step 1 - Connect to all the Database servers.

# Step 2 - Stop all replication services.
sub StopReplication
{
	my $mSQL;
	my $mRet;
	foreach my $Server ( keys %ServerCurRole ) {
		$mSQL = "";
		if ( "BMS" =~ /$ServerPermRole{ $CLP{ "SERVER" } }/ ) {
			if ( $ServerCurRole{ $Server } eq "M" ) {
				#$mSQL = "";
			} # Master Server?

			if ( $ServerCurRole{ $Server } eq "S" ) {
				$mSQL = "STOP SLAVE IO_THREAD;";
			} # Slave Server?

			if ( $CLP{ "TESTRUN" } == 0 && $mSQL ne "" ) {
				$mRet = $ServerConn{ $Server } ->do( $mSQL );
				if ( ! defined( $mRet ) ) {
					LogSQLError( "ERROR", $mSQL );
					$ServerCurRole{ $Server } = "F";
				}
			}
		}
	} # foreach $Server

	LogIt( "STEP 2: Stopping replication services on all servers: Done." );
}


# Step 3 - Make sure that all slaves have processed all statements in their relay log.
# Make sure that all slaves have processed any statements in their relay log.
# On each slave, issue STOP SLAVE IO_THREAD, then check the output of
# SHOW PROCESSLIST until you see 'Has read all relay log'. When this is
# true for all slaves, they can be reconfigured to the new setup.
sub ProcessRelayLogs {
	my ($mMaxTries) = @_;
	
	if ( ! defined( $mMaxTries ) ) {
		my $mMaxTries = 0;
	}
		
	my $mNotAllDone = 1;
	my $mTries = 0;
	my $mSQL = "SHOW PROCESSLIST;";
	while ( $mNotAllDone ) {
		$mNotAllDone = 0;
		foreach my $Server ( keys %ServerCurRole ) {
			if ( "BMS" =~ /$ServerPermRole{ $CLP{ "SERVER" } }/  && $ServerCurRole{ $Server } eq "S" ) {
				# Prepare and Execute the Query Statement
				if ( $CLP{ "TESTRUN" } == 0 ) {
					my $mQuery = $ServerConn{ $Server } ->prepare( $mSQL );
					$mQuery->execute();
					my @mRow;
					while ( @mRow = $mQuery->fetchrow() ) {
						if ( $mRow[ 1 ] eq "system user" || $mRow[ 2 ] eq "system user" ) {
							if ( uc( $mRow[ 6 ] ) =~ /HAS READ ALL RELAY LOG/  ||  uc( $mRow[ 7 ] ) =~ /HAS READ ALL RELAY LOG/) {
								LogIt( "Slave $Server has been successfully stopped." );
								$ServerCurRole{ $Server } = "X";
							} else {
								LogIt( "Slave $Server is still processing its relay log." );
								$mNotAllDone = 1;
							}
						} # system user?
					} # while mRow
					# Close the Query Statement Handle
					$mQuery->finish();
				} # TESTRUN?
			} # Slave?
		} # foreach $Server

		$mTries ++;
		if ( $mNotAllDone == 1 && $mMaxTries > 0) {
			if ($mTries <= $mMaxTries) {
				LogIt( "Pausing 3 secs so one or more slaves can process its relay log. ($mTries)" );
				sleep( 3 );
			} else {
				exit ( 1 );
			}
		} elsif ( $mNotAllDone == 1 && $mMaxTries == 0 ) {
			LogIt( "Pausing 5 secs so one or more slaves can process its relay log. ($mTries)" );
			sleep( 5 )
		}
	} # while

	LogIt( "STEP 3: STEP 3: Making sure that all slaves have processed their relay log: Done." );
}# Step 3 - Make sure that all slaves have processed any statements in their relay log.


# Step 4 - Get the replication status for each server.
# This is for notation just in case things are seriously wrong.
sub GetReplicationStatus
{
	foreach my $Server ( keys %ServerCurRole ) {
		if ($ServerCurRole{ $Server } eq "F") { next; }
	
		my $mSQL = "";
		if ( "BMS" =~ /$ServerPermRole{ $CLP{ "SERVER" } }/  ) {

			if ( $ServerCurRole{ $Server } eq "M" ) {
				$mSQL = "SHOW MASTER STATUS;";
			} # Master?

			# NOTE: The current role may have changed in step 3 from "S" to "X"
			# to mark that it was successfully STOPPED.
			# If it's still "S" that means that the slave thread had already stopped for
			# some other reason. Check the MySQL error log on that slave.
			if ( $ServerCurRole{ $Server } eq "S" || $ServerCurRole{ $Server } eq "X" || $ServerCurRole{ $Server } eq "R") {
				$mSQL = "SHOW SLAVE STATUS;";
			} # Slave?

			# Prepare and Execute the Query Statement
			if ( $CLP{ "TESTRUN" } == 0 ) {
				my $mQuery =$ServerConn{ $Server } ->prepare( $mSQL );
				$mQuery->execute();
				my @mRow;
				my $strLog ='';
				while ( @mRow = $mQuery->fetchrow() ) {
					foreach (@mRow) {
						if (defined $_) { $strLog .= $_. '|'; }
					}
					LogIt( "$Server: $strLog");
				} # while mRow
				# Close the Query Statement Handle
				$mQuery->finish();
			} # TESTRUN?
		} # Slave?
	} # foreach $Server

	LogIt( "STEP 4: Getting the replication status for each server: Done." );
}# Step 4 - Get the replication status of each server.

# Step 5 - On the slave being promoted to master,
# issue STOP SLAVE and RESET MASTER.
sub ConfigureNewMaster
{
	my ($NewMaster) = @_;
	my $mSQL = "";
	my $mRet;
	if ( $CLP{ "TESTRUN" } == 0 ) {
		$mSQL = "STOP SLAVE;";
		$mRet = $ServerConn{ $NewMaster } ->do( $mSQL );
		if ( ! defined( $mRet ) ) {
			LogSQLError( "FATAL", $mSQL );
			exit( 1 );
		}

		$mSQL = "RESET MASTER;";
		$mRet = $ServerConn{ $NewMaster } ->do( $mSQL );
		if ( ! defined( $mRet ) ) {
			LogSQLError( "FATAL", $mSQL );
			exit( 1 );
		}
	} # TESTRUN

	$ServerCurRole{ $NewMaster }  = "M";

	# If the old master isn't Down/Disabled or Failed, then change its current role to slave.
	if ( $OldMaster ne "" &&  "DF" !~ /$ServerCurRole{ $OldMaster }/ && $ServerCurRole { $OldMaster } ne $ServerCurRole{ $NewMaster } ) { 
		$ServerCurRole{ $OldMaster }  = "S"; 
	}

	LogIt( "STEP 5: Configuring new master server $NewMaster: Done." );
} # Step 5 - issue STOP SLAVE and RESET MASTER.


# Step 6 - On the other slaves use STOP SLAVE and
# CHANGE MASTER TO MASTER_HOST='NewMaster'
sub ConfigureSlaves
{
	my ($NewMaster) = @_;
	my $mSQL;
	my $mRet;
	foreach my $Server (  keys %ServerPermRole ) {
		$mSQL = "";
		# Only process servers that are permissible to be a slave.
		if ( "SB" =~ /$ServerPermRole{ $Server }/  &&  $ServerCurRole{ $Server } ne "F" ) {
			# Don't slave the new master!
			if ( "$Server" ne "$NewMaster" ) {
				LogIt( "Reconfiguring slave server $Server." );

				# Stop the slave.
				$mSQL = "STOP SLAVE;";
				if ( $CLP{ "TESTRUN" } == 0 && $ServerCurRole{ $Server } ne "F" ) {
					$mRet = $ServerConn{ $Server } ->do( $mSQL );
					if ( ! defined( $mRet ) ) {
						LogSQLError( "ERROR", $mSQL );
						$ServerCurRole{ $Server } = "F";
					}
				}

				# Change the master.
				$mSQL = "CHANGE MASTER TO MASTER_HOST='" . $NewMaster . "', MASTER_PORT=" . $ServerPort{ $NewMaster } . ", MASTER_USER='" . $ServerMasterUser{ $Server } . "', MASTER_PASSWORD='" . $ServerMasterPass{ $Server } . "';";
				
				if ( $CLP{ "TESTRUN" } == 0 && $ServerCurRole{ $Server } ne "F" ) {
					$mRet = $ServerConn{ $Server } ->do( $mSQL );
					if ( ! defined( $mRet ) ) {
						LogSQLError( "ERROR", $mSQL );
						$ServerCurRole{ $Server } = "F";
					}
				}	

				# Start the slave.
				$mSQL = "START SLAVE;";
				if ( $CLP{ "TESTRUN" } == 0 && $ServerCurRole{ $Server } ne "F" ) {
					$mRet = $ServerConn{ $Server } ->do( $mSQL );
					if ( ! defined( $mRet ) ) {
						LogSQLError( "ERROR", $mSQL );
						$ServerCurRole{ $Server } = "F";
					}
				}

				# Successfull update?
				if ( $ServerCurRole{ $Server } ne "F" ) {
					# Update this server to it's new current role.
					$ServerCurRole{ $Server } = "S";
				}

			} #NewMaster?
		} # Slavable?
	} # foreach $Server

	LogIt( "STEP 6: Changing slaves to new master server $NewMaster: Done." );
}# Step 6 - On the other slaves use STOP SLAVE and CHANGE MASTER TO MASTER_HOST='NewMaster'


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

  	foreach my $Server (  keys %ServerPermRole ) {
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
       		$ErrorCount = $ErrorCount + 1;
     		LogIt( "WARNING! $Server had a problem and was not successfully modified." );
     	}
	} # foreach $Server

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
	if ( $CLP{ "TESTRUN" } == 0 && $ConfigCreated == 1) {
     	while (my ($login, $config_path) = each %WebClients) {
         	#-- scp file to web clients and cature STDERR as well as STDOUT
     		my $config_file = $config_path . basename($CLP{ 'DBCONFIGFILE' });
    		my $exe_cmd = "scp $TmpFile $login:$config_file";
     		my $exec_result = `$exe_cmd 2>&1`;
         	if ($? == 0) {
         		LogIt( "Successfully updated $login $config_file." );
        	} else {
				$status = 0;
				LogIt ( "WARNING! Failed to update $login $config_file." );
      		}
   		}
  	}

	LogIt( "Distribute database config file $TmpFile: Done." );
	return $status;
}# Distribute drupal db config file


#
# Subroutines
##############################################################################

# (end of file)

