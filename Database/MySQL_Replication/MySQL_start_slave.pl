#!/usr/bin/perl
# MySQL_start_slave.pl
# Version 1.0 beta
# Last Modified 2012-07-17
# Originally written by: Van Stokes Jr
#
# This script will start slave replication
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
#
# servers.dat format is:
#   Host : Port : User : Password : PR : CR
#
# Example servers.dat:
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
#   F = Failed/Error (something went wrong during process).
#
# Example execution:
# For testing:
#    perl MySQL_start_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat --startup_status=S --test
# For real:
#    perl MySQL_start_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat --startup_status=S
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
my %WebClients;
my $mSQL;
my $mRet;
my $Server;   
my %ServerPort;
my %ServerMasterUser;
my %ServerMasterPass;
my %ServerPermRole;
my %ServerCurRole;
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
$CLP{ "STARTUP_STATUS" }  = "S";

# Parse the command line arguments
parse_command_line();

# Load the DB server list.
load_db_server_list ($CLP{ 'DBCONFIGFILE' });

# Load the FE server list.
load_fe_server_list ($CLP{ 'FECONFIGFILE' });

# Validation
validate();

# Step 1 - Connect to the slave server.
connect_db ($CLP{ 'SERVER' });

# Step 2 - Start the slave server.
start_slave ($CLP{ 'SERVER' }, $CLP{ 'STARTUP_STATUS' });

# Write out new db dat file with new configuration.
write_config($TmpDBDataFile);

 # Distribute drupal db config file
distribute_config($TmpDBDataFile);


LogIt( "Successfully started slave server $CLP{ 'SERVER' }." );
LogIt( "$ErrorCount slave NOT updated because a problem ocurred." );

if ( $CLP{ "TESTRUN" } ) {
	LogIt( "     ***** TEST RUN, NOTHING WAS MODIFIED *****" );
}


exit( 0 );

##############################################################################
# Subroutines
#

# Parse the command line arguments
# perl MySQL_start_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat  --test
# For 
sub parse_command_line
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
		
		if ( uc( $mField[0] ) eq "--STARTUP_STATUS" ||  uc( $mField[0] ) eq "-SS" ) {
			if ( uc( $mField[1] ) eq "S" || uc( $mField[1] ) eq "R" || uc( $mField[1] ) eq "X") {
				$CLP{ "STARTUP_STATUS" } = $mField[1];
			}
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
sub load_db_server_list
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

			# Is this a slave that needs restarting?
			if ( "BS" =~ /$ServerPermRole{ $mField[0] }/  && $ServerCurRole{ $mField[0] } eq "X" ) {
				$SlaveCount = $SlaveCount + 1;
				$DBSlaves{ $mField[0] } = $mField[0];
				LogIt( "$mField[0] is currently a slave that needs restarting." );
			} #valid slave?
		} 
	} # while
	close( $DBFileHandle );
}# Load the DB server list.

# Load the FE server list.
sub load_fe_server_list
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
sub validate
{
	# Make sure we have sufficient servers configured in the dat file.	
	if  ( $BothCount < 2 || ( $BothCount < 1 && $MasterCount < 1 || $SlaveCount < 1 )) {
		LogIt( "FATAL! Insufficient servers configured in $CLP{ 'DBCONFIGFILE' }." );
		LogIt( "There must be at least: one (PR:B) as master and one (PR:B) as a slave." );
		UsageHint();
		exit( 1 );
	}

	# Make sure we have slave servers in dat file
	if (scalar(keys %DBSlaves) < 1) {
		LogIt( "FATAL! No Slave servers require restart configured in $CLP{ 'DBCONFIGFILE' }." );
		LogIt( "There must be at least: one (PR:B) as a slave." );
		UsageHint();
		exit( 1 );
	}

	# If we have not specified a slave server. If not, then we get the first one in dat file
	if ( !$CLP{ "SERVER" } || $CLP{ "SERVER" }  eq "" || $ServerCurRole{ $CLP{ "SERVER" } } ne "X") {
		$CLP{ "SERVER" } = (keys %DBSlaves)[0];
		LogIt( "NOTICE! Set slave server as $CLP{ 'SERVER' } from $CLP{ 'DBCONFIGFILE' } file." );
	}

} # Validation

# Step 1 - Connect to the slave server.
sub connect_db
{
	my ($DBServer) = @_;
	LogIt( "STEP 1: Connecting to $DBServer slave server." );
	my $MyDBIURL = "";
	my $MyDBIOpts = { PrintError => 0 };
	if ( "BS" =~ /$ServerPermRole{ $DBServer }/ && $ServerCurRole{ $DBServer } ne "F" ) {
		LogIt( "Connecting to server $DBServer:$ServerPort{ $DBServer } as user $CLP{ 'USER' }." );
		if ( $CLP{ "TESTRUN" } == 0 ) {
			$MyDBIURL = "DBI:mysql:mysql:$DBServer:$ServerPort{ $DBServer };mysql_connect_timeout=5";
			$ServerConn{ $DBServer } = DBI->connect( $MyDBIURL, $CLP{ "USER" }, $CLP{ "PASSWORD" }, $MyDBIOpts );

			if ( ! defined( $ServerConn{ $DBServer } ) ) {
				# Connection failed to the server.
				LogIt( "ERROR! Failed to connect to $DBServer:$ServerPort{ $DBServer }. The error was:" );
				LogIt( "$DBI::errstr" );
				$ServerCurRole{ $DBServer } = "F";
			} 
		} # TESTRUN?
	}

	# Make sure we have a connection to the slave server!
	if ( $CLP{ "TESTRUN" } == 0 && $ServerCurRole{ $DBServer } eq "F" ) {
		LogIt( "FATAL! Not connected to the requested slave server $DBServer." );
		exit( 1 );
	}

	LogIt( "STEP 1: Connecting to $DBServer ($ServerCurRole{ $DBServer }) slave server: Done." );
} # Step 1 - Connect the slave server.

# Step 2 - Start the slave server.
sub start_slave
{
	my ($DBServer, $DBStatus) = @_;
	LogIt( "STEP 2: Starting the slave server $DBServer." );
	my $mSQL="";
	my $mRet="";
	if ( "BS" =~ /$ServerPermRole{ $DBServer }/ ) {
		if ( $ServerCurRole{ $DBServer } eq "X" ) {
			$mSQL = "START SLAVE;";
		} # Slave Server?

		if ( $CLP{ "TESTRUN" } == 0 && $mSQL ne "" ) {
			$mRet = $ServerConn{ $DBServer } ->do( $mSQL );
			if ( ! defined( $mRet ) ) {
				LogSQLError( "ERROR", $mSQL );
				$ServerCurRole{ $DBServer } = "F";
			}
		}
	}
	
	# Successfull update?
	if ( $ServerCurRole{ $DBServer } ne "F" ) {
		# Update this server to it's new current role.
		 LogIt( "Slave $DBServer has been successfully started." );
		$ServerCurRole{ $DBServer } = $DBStatus;
	}

	LogIt( "STEP 2: Starting the slave server $DBServer: Done." );
} # Step 2 - Start the slave server.


# Write out new db dat file with new configuration.
sub write_config
{
	my ($TmpFile) = @_;

	$ErrorCount = 0;
	$SlaveCount = 0;

	if ( $CLP{ "TESTRUN" } == 0 ) {
		LogIt( "Updating $TmpFile with new role information." );
		open( TmpDBFileHandle, ">", $TmpFile ) or die "FATAL! Can't open the $TmpFile for writing!\n";
		flock( TmpDBFileHandle, 2);
	} else {
		LogIt( " " );
		LogIt( "$TmpFile file will look like this:" );
	} # $CLP{ "TESTRUN" }

	foreach my $SlaveServer (  keys %ServerPermRole ) {
		if ( $CLP{ "TESTRUN" } == 0 ) {
			# Close MySQL Handle/Connection
			if ( defined( $ServerConn{ $SlaveServer } ) ) {
				$ServerConn{ $SlaveServer }->disconnect();
			}
			# update the tmp db .dat file.
			print( TmpDBFileHandle "$SlaveServer:$ServerPort{ $SlaveServer }:$ServerMasterUser{ $SlaveServer }:$ServerMasterPass{ $SlaveServer }:$ServerPermRole{ $SlaveServer }:$ServerCurRole{ $SlaveServer }\n" );
		} else {
			LogIt( "$SlaveServer:$ServerPort{ $SlaveServer }:$ServerMasterUser{ $SlaveServer }:$ServerMasterPass{ $SlaveServer }:$ServerPermRole{ $SlaveServer }:$ServerCurRole{ $SlaveServer }" );
		}

		# Is this a slave server?
		if ( $ServerCurRole{ $SlaveServer } eq "S" )	{
			$SlaveCount = $SlaveCount + 1;
		}

		# Did we have problems with this server?
		if ( $ServerCurRole{ $SlaveServer } eq "F" )	{
			$ErrorCount = $ErrorCount + 1;
			LogIt( "WARNING! $SlaveServer had a problem and was not successfully modified." );
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
sub distribute_config
{
 	my ($TmpFile) = @_;
	my $status = 1;

  	LogIt( "Distribute database config file $TmpFile." );
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


# Show the programs usage.
sub Usage
{
	print( "\n" );
	print( "Usage:  MySQL_start_slave.pl --server=slavedb --user=MyUser --password=MyPassword --dbconfigfile=drupal_db_servers.dat --feconfigfile=drupal_fe_servers.dat --startup_status=S\n" );
	print( " --server=, -s=          (r) the slave server to start.\n" );
	print( " --user=, -u=            (r) a MySQL user with privileges to make rep changes.\n" );
	print( " --password=, -p=        (r) the password for --user.\n" );
	print( " --dbconfigfile=, -d=    (r) the drupal database config file.\n" );
	print( " --feconfigfile=, -f=    (r) the drupal front-end config file.\n" );
	print( " --startup_status=, -ss= (o) the status flag which will be set when slave is restarted (default S).\n" );
	print( " --test, -t              (o) perform a test run altering nothing.\n" );
	print( " --quiet, -q             (o) quiet output (outputs nothing).\n" );
	print( " --help, -h, -?          (o) this usage information.\n" );
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

#
# Subroutines
##############################################################################

# (end of file)

