#!/usr/bin/perl -w

# Check historical data, compute an average over any 24-hour period (not just aligned with day boundaries) for violations

use Data::Dumper;
use POSIX;
use envlib;
use Getopt::Std;

my %opts;

getopts('y:s:t:', \%opts);

my $year = $opts{'y'} || 2016;
my $sensor = $opts{'s'} || 'BENZN';
my $threshold = $opts{'t'} || 3.9;

my %seen;                       # key is $station:from:to

my $hist_data = slurp_subdir_multistation("envdata_hist/${year}/");

#my @stations = ("Kiryat Haim-Degania", "Igud (check-post)", "Kiryat Binyamin");
my @sensors = ($sensor);

my $period = 24;		# hopefully, each reading is one hour

for my $station (keys %$hist_data) {
  for my $sensor (@sensors) {
    my $d = $hist_data->{$station}->{$sensor}->{"reads"};
    my @ts_list =  sort {$a <=> $b } keys %$d;
    my @val_list = map { $d->{$_} } @ts_list;
    my $last_idx = $#ts_list - $period + 1;
    for(my $i=0; $i<=$last_idx; $i++) {
      my @vals = @val_list[$i..($i+$period-1)];
      my ($avg, $has_undef) = avg(\@vals);
      if($avg > $threshold) {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($ts_list[$i]);
	print join("\t", $station, $sensor, sprintf("%02d/%02d/%4d %02d:%02d", $mday, 1+$mon, 1900+$year, $hour, $min), sprintf("%.2f", $avg), $has_undef ? 0 : 1, @vals) . "\n";
      }
    }
  }
}

sub avg {
  my ($v) = @_;
  my $sum = 0; my $num = 0;
  my $has_undef = 0;
  map {
    my ($vv) = $_;
    if(defined($vv)) {
      $num++;
      $sum += $vv;
    } else {
      $has_undef = 1;
    }
  } @$v;
  if($num > 0) {
    return ($sum/$num, $has_undef);
  } else {
    return ($sum/$num, $has_undef);
  }
}
