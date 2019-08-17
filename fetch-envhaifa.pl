#!/usr/local/bin/perl5.24.3 -w

# download environmental sensor data

use WWW::Mechanize;
use WWW::Mechanize::PhantomJS;
use File::Path qw/make_path/;
use Time::Local;
use POSIX;
use Getopt::Std;
use DateTime;

use Data::Dumper;

# Note to self: there is also an API:
# http://www.envihaifa.net/api/stations
# http://www.envihaifa.net/api/stations/38?getLatestValue=true&type=json
# But it doesn't include all stations that the human front-end has (for example Gadiv new)

getopts("d:o:", \%options);

my $date = $options{'d'};
my $OUTDIR_BASE = $options{'o'} || ".";
my $now = time();

if (!defined($date)) {		# use today
  my $date = DateTime->now()->dmy('/');
  dump_date($date);
} elsif($date =~ m!^(\d{4})(\d\d)$!) { # do a full month
  my ($start_year, $start_month) = ($1, $2);
  process_month($start_year, $start_month);
} elsif($date eq 'prevmonth') {
  my $date = DateTime->now();
  $date->truncate(to => 'month');
  $date->subtract(months => 1);
  process_month($date->year(), $date->month());
} else {
  dump_date($date);
}

sub process_month {
  my ($start_year, $start_month) = @_;
  my $dt = DateTime->new(
			 year       => $start_year,
			 month      => $start_month,
			 day        => 1,
			);
  do {
    my $date_to_process = $dt->dmy('/');
    dump_date($date_to_process);
    $dt->add(days => 1);
  } while(($dt->epoch() < $now) && ($dt->month() == $start_month) && ($dt->year() == $start_year));
}

sub dump_date {
  my $success = 0;
  my $to_retry = 1;
  my $max_retries = 3;
  my $num_retries = 0;
  while($to_retry && $num_retries++ < $max_retries) {
    eval {
      undef $to_retry;
      dump_date_once(@_);
    };
    if($@) {
      if ($@ =~ /^(Server returned error message read timeout|An operation did not complete before its timeout expired|No data)/i) {
        $to_retry = 1;
        # print STDERR "will retry\n";
      } else {
        warn $@;
      }
    } else {
      $success = 1;
    }
  }
  warn "Failed to retrieve" unless($success);
}

sub dump_date_once {
  my ($date) = @_;
  my $date_nextday;

  my ($outdir, $outfile);
  if ($date =~ m!^(\d+)/(\d+)/(\d+)$!) {
  my $dt = DateTime->new(
			 year       => $3,
			 month      => $2,
			 day        => $1,
			);

  my $dt_nextday = $dt->clone();
  $dt_nextday->add(days => 1);
  $date_nextday = $dt_nextday->ymd('/');
  # figure out output directory and filename
  $outdir = sprintf("%s/%s", $OUTDIR_BASE, $dt->ymd('/'));
  $outfile = "${now}.html";
  } else {
    die "Bad date format $date\n";
  }

  # print STDERR "$date $date_nextday\n";
  
  #  my $mech = WWW::Mechanize::PhantomJS->new('launch_exe' => 'bin/phantomjs');
  my $mech = WWW::Mechanize::PhantomJS->new();  #'launch_exe' => 'bin/phantomjs');

  $mech->get('http://www.envihaifa.net/frmMultiStationReport.aspx');
  die "No url" unless($mech->success());

  my $driver= $mech->driver();
  
  $mech->form_id('form1');

  # choose output format: "table"
##  print $mech->content;
  $mech->click_button(id => 'RadioButtonList1_0');
  sleep(2);
  # choose time range type: "interval"
  $mech->click_button(id => 'RadioButtonList2_3');

  # Date
  # start date
  my $f = $mech->by_id('BasicDatePicker1_TextBox', single => 1, frames => 1);
  $mech->click_button(id => 'BasicDatePicker1'); # no idea why this is needed, but it is
  $mech->field($f, $date);
  if (0) { # for the life of me, I can't set the time, only the date (and the time on the one-station report - go figure)
    my $input_box = $driver->find_element_by_id('txtStartTime');
    $driver->mouse_move_to_location(element => $input_box);
    $driver->click();
    $driver->send_keys_to_active_element('04:00');
    $mech->field('txtEndTime', '12:00');
    sleep(2);
  }
  # end date
  $f = $mech->by_id('BasicDatePicker2_TextBox', single => 1, frames => 1);

  $mech->click_button(id => 'BasicDatePicker2'); # no idea why this is needed, but it is
  $mech->field($f, $date_nextday);

  # reporting statistic: average
  $mech->field('ddlAvgType', 'ממוצע');

  # reporting frequency: hourly
  $mech->field('ddlTimeBase', '60');

  # choose all stations, all sensors
  $mech->click_button(id => 'chkAll');

  $mech->click('btnGenerateReport');
  die unless($mech->success());

  if ($mech->content =~ m!var strHTML\s*=\s*'(<html>.*</body></html>)';!) {
    my $html_table = $1;
    if(! -d $outdir) {
      make_path($outdir) or die "Can't create path $outdir";
    }
    open(F, ">${outdir}/${outfile}") or die "Can't create file $outfile";
    binmode(F, ":utf8");
    print F $html_table;
    close(F);
    if(system("gzip ${outdir}/${outfile}") != 0) {
      die "Can't gzip ${outdir}/${outfile}";
    }
  } else {
    die "No data";
  }
}
