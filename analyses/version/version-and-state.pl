#!/usr/bin/perl -w

# Given a log of observations, compute the lag between first download time and reported observation time

use envlib;
use Data::Dumper;
use List::Util qw/sum/;

# keyed by date, and station:sensor, stores a list of lag values in seconds
# at post-processing, each list is replaced by its mean
my %lags;

my $data = slurp_subdir_multistation("envdata/2016");
my %all_names;

for my $station (keys %$data) {
  my @sensors = keys %{$data->{$station}};
  for my $sensor (@sensors) {
    my $h = $data->{$station}->{$sensor}->{'version'};
    for my $ts (keys %$h) {
      my $dt_now = DateTime->from_epoch(epoch => $ts, time_zone => $envlib::TZ_HERE);
      my $day = $dt_now->ymd('');
      my $delta = $h->{$ts} - $ts;
      my $key = "$station:$sensor";
      push(@{$lags{$day}->{$key}}, $delta);
      $all_names{$key} = 1;
    }
  }
}

# post process: turn lists of values into averages

for my $day (keys %lags) {
  for my $key (keys %{$lags{$day}}) {
      my $ll = $lags{$day}->{$key};
      my $mean = sum(@$ll) / scalar(@$ll);
      $lags{$day}->{$key} = $mean;
    }
}

my @all_sensors = sort keys(%all_names);

print join("\t", (qw/date/, @all_sensors)) . "\n";

for my $day (sort keys %lags) {
  my @v = ($day);
  for my $key (@all_sensors) {
    my $vv = $lags{$day}->{$key};
    push(@v, defined($vv) ? sprintf("%.0f", $vv) : "");
  }
  print join("\t", @v) . "\n";
}
