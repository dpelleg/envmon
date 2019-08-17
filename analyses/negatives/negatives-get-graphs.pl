#!/usr/bin/perl -w

use POSIX;
use WWW::Mechanize;
use WWW::Mechanize::PhantomJS;
use File::Path qw/make_path/;
use Time::Local;
use File::Temp qw/ tempfile tempdir /;
use Data::Dumper;
use List::Util qw(first max maxstr min minstr reduce shuffle sum);
use Encode;
use Getopt::Std;
use DateTime;
use envlib;

my $OUTDIR_BASE = "negatives";

%mon2num = qw(
  jan 1  feb 2  mar 3  apr 4  may 5  jun 6
  jul 7  aug 8  sep 9  oct 10 nov 11 dec 12
	    );

my %periodmap = (
		 'daily' => {'days_back' => 0, 'button' => 'RadioButtonList2_0'},
		 'weekly' => {'days_back' => 6, 'button' => 'RadioButtonList2_1'},
		 );

my %xlat_sensor = (
		   'חנקן חד-חמצני' => 'NO',
		   'חנקן דו-חמצני' => 'NO2',
		   'אוזון' => 'O3',
		   'חלקיקי 10 מיקרון' => 'PM10',
		  );

# process output of negatives.pl, fetch and store graphs
while(<ARGV>) {
  chomp;
  if(/^min (\S+) (.*) (\S+): mean (\S+) from (.*) to (.*) for (\d+) hours$/) {
    my ($min_val, $station, $sensor, $mean_val, $start_ts, $end_ts, $duration) = ($1, $2, $3, $4, $5, $6, $7);
    if($station =~ /^(.*) NOx$/ && $sensor eq 'Dry') {
      $station = $1;
      $sensor = "NOx ".$sensor;
    }
    $start_ts = str_to_ts($start_ts);
    $end_ts = str_to_ts($end_ts);
    next if($min_val > -10);
    next if($sensor =~ /^((LAF|LXpk)_(max|min)|L[XA]eq|LAim|TEMP|ITemp)$/);
    print("min $min_val $station/$sensor " . ($start_ts) . " - " . ($end_ts). "\n");
    my $today = $start_ts;
    my $done;
    do {
      my $seen_key = join(":", $station, $sensor, $start_ts);
      if($seen{$seen_key}) {
	# nothing
      } else {
	$seen{$seen_key} = 1;
	
	my $chart = fetch_sensor($today->dmy('/'), $station, $sensor, 'daily');
	print "$chart\n";
      }
      print $today->ymd . "\n";
      if($today->ymd eq $end_ts->ymd) {
	$done = 1;
      }
      $today->add(days => 1);
    } while(!$done);

  }
  
}

sub str_to_ts {
  my ($s) = @_;
  my $ret;
  if($s =~ /(\w+) (\w+) +(\w+) (\w+):(\w+):(\w+) (\d+)/) {
    my ($wday, $mon, $mday, $h, $m, $s, $year) = ($1, $2, $3, $4, $5, $6, $7);
    $ret = DateTime->new(
			 year       => $year,
			 month      => $mon2num{lc($mon)},
			 day        => $mday,
			 hour       => $h,
			 minute     => $m
			);
  }
  #return $ret->epoch();
  #return $ret->dmy('/');
  return $ret;
}


# download environmental sensor data: get the daily/weekly report of a given station/sensor as a PNG file
sub fetch_sensor {
  my (@args) = @_;
  return insist(\&fetch_sensor_internal, @args);
}

sub fetch_sensor_internal {
  my ($date, $station, $sensor, $period) = @_;
  my $graph_start_date;

  $period = 'daily' unless(defined($period));
  die "Unknown period $period" unless(exists($periodmap{$period}));
  my ($outdir, $outfile, $outpath);
  if ($date =~ m!^(\d+)/(\d+)/(\d+)$!) {
    my $dt = DateTime->new(
			   year       => $3,
			   month      => $2,
			   day        => $1);
    $graph_start_date = $dt->clone();
    $graph_start_date->subtract(days => $periodmap{$period}->{'days_back'});

    # figure out output directory and filename
    $outfile = sprintf("%s_%s_%s_%s.png", $dt->strftime("%Y%m%d%H%M%S"), $station, $sensor, $period);
    $outdir = sprintf("%s/", $OUTDIR_BASE);
    $outpath = sprintf("%s%s", $outdir, $outfile);
  } else {
    warn "Bad date format $date\n";
    return undef;
  }

  if(-f $outpath) {             # output already exists from a previous run
    return $outpath;
  }
  # print STDERR "$date $date_nextday\n";
  
  my $mech = WWW::Mechanize::PhantomJS->new('launch_exe' => 'bin/phantomjs');

  $mech->get('http://www.envihaifa.net/frmStationReport.aspx');
  return undef unless($mech->success());

  my $driver= $mech->driver();
  
  $mech->form_id('form1');

  # choose output format: "graph"
  $mech->click_button(id => 'RadioButtonList1_1');
  # choose time range type: "daily"/"weekly"/etc
  $mech->click_button(id => $periodmap{$period}->{'button'});

  # Date
  # start date
  $mech->click_button(id => 'BasicDatePicker1'); # no idea why this is needed, but it is
  sleep(1);
  my $f = $mech->by_id('BasicDatePicker1_TextBox', single => 1, frames => 1);
  $mech->field($f, $graph_start_date->dmy('/'));
  # no need for end date
  
  # reporting statistic: average
  $mech->field('ddlAvgType', 'ממוצע');
  sleep(1);
  
  my $station_map = get_select_map('ddlStation', encode_utf8($mech->content()));
  my $station_key = $station_map->{$station};
  unless(defined($station_key)) {
    warn "Unknown station $station\n";
    return undef;
  }

  # choose station
  #$mech->click_button(id => 'ddlStation');
  $mech->field('ddlStation', $station_key);
  $mech->eval("setTimeout('__doPostBack(\\'ddlStation\\',\\'\\')', 0)");
  sleep(2);

  # choose sensor
  my $sensor_map = get_sensor_map($mech->content());

  my $sensor_key = $sensor_map->{$sensor};
  unless(defined($sensor_key)) {
    warn "Unknown sensor $sensor for station $station\n";
    return undef;
  }

  # un-choose all stations, all sensors
  $mech->click_button(id => 'chkAll');

  $mech->click_button(id => $sensor_key);

  # reporting frequency: hourly
  $mech->field('ddlTimeBase', '60');

  $mech->click('btnGenerateReport');
  return undef unless($mech->success());
 
  if ($mech->content =~ m!<img id="C1WebChart1"!) {
    if(! -d $outdir) {
      make_path($outdir) or die "Can't create path $outdir";
    }
    $driver->capture_screenshot($outpath) or die "Can't save screenshot $outpath";
  } else {
    return undef;
  }
  return $outpath;
}

sub get_select_map {
  my ($inputname, $content) = @_;
  my %ret;
  if($content =~ m!<select \s*name\s*=\s*"${inputname}"(.*?)</select>!s) {
    my $sel = $1;
    while($sel =~ m!value="(\w+)">\s*([^<]+)</option>!g) {
      my ($key, $value) = ($1, $2);
      $ret{translate_station(encode_utf8($value))} = $key;
    }
  } else {
    warn "Bad form";
    return undef;
  }
  return \%ret;
}

sub get_sensor_map {
  my ($content) = @_;
  my %ret;
  while($content =~ m!\<input([^>]*)\>!g) {
    my $inner = $1;
    my %pairs;
    while($inner =~ m!(\w+)\s*=\s*"([^"]*)"!g) {
      $pairs{$1} = $2;
    }
    if($pairs{'type'} eq 'checkbox' && exists($pairs{'title'})) {
      my $key = encode_utf8($pairs{'title'});
      my $val = $pairs{'id'};
      if(exists($xlat_sensor{$key})) {
	$key = $xlat_sensor{$key};
      }
      $ret{$key} = $val;
    }
  }
  return \%ret;
}

