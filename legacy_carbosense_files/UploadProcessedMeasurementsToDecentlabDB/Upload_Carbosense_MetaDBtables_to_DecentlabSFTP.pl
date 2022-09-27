#!/usr/bin/perl
#--------------
#
# - uploads Carbosense DB dump file (Metadata) on Decentlab SFTP server
# - dir: "/project/CarboSense/MySQL_dump"
#

# use/libs
use Net::SFTP::Foreign;

# Decentlab SFTP information
$host     = "swiss.co2.live";
$port     = 54322;
$user     = "empa503";
$password = "bAMja9mCaB";

# directories
$EmpaDir = "/project/CarboSense/MySQL_dump";

# read all available files in dump directory
opendir(DIR, $EmpaDir) or die $!;
@filesInEmpaDir = readdir(DIR);
closedir(DIR);
    

# Open SFTP connection
my $sftp = Net::SFTP::Foreign->new($host,user=>$user,password=>$password,port=>$port);
$sftp->die_on_error("Unable to establish SFTP connection");
$sftp->setcwd("mysql-daily-dump");


# Upload newest dump-file
$transmitted  = 0;
$days_back    = 0;
$epoc_UTC_now = time();

while($transmitted==0 & $days_back<7){

  $epoc_UTC = $epoc_UTC_now - $days_back * 86400;
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($epoc_UTC);
  $date_str = sprintf("%04d%02d%02d", $year+1900, $mon+1, $mday);
  
  for($i=0;$i<scalar(@filesInEmpaDir);$i++){
    if(@filesInEmpaDir[$i]=~$date_str && @filesInEmpaDir[$i]=~"CarboSense_meta_"){
      $file2upload = "${EmpaDir}/@filesInEmpaDir[$i]";
      
      # transmit file (SFTP)
      $sftp->put($file2upload, @filesInEmpaDir[$i], overwrite => 0);
      $transmitted = 1;
    }
  }
  
  $days_back = $days_back + 1; 
}

# Close FTP connection
$sftp->disconnect;

# exit
exit 0;