#!/usr/bin/perl -w

# parse the HTML table read by fetch-envhaifa.pl, report negative readings

use Data::Dumper;
use POSIX;
use envlib;
use Getopt::Std;

my %opts;

getopts('g:', \%opts);

if (1) {

  #my $data = slurp_sensor_table_csv("envdata_hist/envhaifa-201501.csv");
  my $data = slurp_subdir_multistation("envdata_hist/2017");

  for my $station (keys %$data) {
    for my $sensor (keys %{$data->{$station}}) {
      my $slice = $data->{$station}->{$sensor}->{"reads"};
      my $state = "pos";
      my $first_ts;
      my $last_ts;
      my @readings = ();
      for my $ts (sort {$a <=> $b } keys %$slice) {
	if ($slice->{$ts} < -1) { # up to -1 considered measurement error
	  if ($state eq 'pos') { # positive -> negative transition
	    $first_ts = $ts;
	    $state = 'neg';
	    push(@readings, $slice->{$ts});
	  } else {
	    $last_ts = $ts; # negative -> negative, update last timestamp
	    push(@readings, $slice->{$ts});
	  }
	} else {
	  if ($state eq 'pos') { # positive -> positve, do nothing
	  } else {	     # negative -> positive transition, output
	    output($station, $sensor, $first_ts, $ts, \@readings);
	    $state = 'pos';
	    undef $first_ts;
	    undef $last_ts;
	    @readings = ();
	  }
	}
      }
      if($state eq 'neg') {
	output($station, $sensor, $first_ts, undef, \@readings);
      }
    }
  }
}

sub output {
  my ($station, $sensor, $first_ts, $last_ts, $readings) = @_;
  my $sum = sum_list($readings);
  my $N = scalar(@$readings);
  my $min = min_list($readings);
  my $avg = $N > 0 ? sprintf("%.1f", $sum/$N) : "-";
  print "min $min $station $sensor: mean $avg from " . scalar(localtime($first_ts));
  if (defined($last_ts)) {
    print " to " . scalar(localtime($last_ts));
    my $lag = $last_ts - $first_ts;
    $lag /= 3600;
    print " for $lag hours";
  }
  print "\n";
}

sub sum_list {
  my ($l) = @_;
  my $ret = 0;
  map { $ret += $_ } @$l;
  return $ret;
}

sub min_list {
  my ($l) = @_;
  my @l = @$l;
  my $ret = shift(@l);
  map { $ret = $_  if($_ < $ret) ;} @l;
  return $ret;
}
