#!/usr/bin/perl -w

# Given a log of observations, find suspicious lags between first download time and reported observation time

use envlib;
use Data::Dumper;

my $station = "Nesher";
my $sensor = 'NO2';
#my $station = "Kiryat Haim-Degania";
my $period_start = "20/10/2016 12:00";
my $period_hours = 10;

my $HOUR = 3600;

my $data = slurp_subdir_multistation("envdata/2017/01");

for my $station (keys %$data) {
  my @sensors = keys %{$data->{$station}};
  for my $sensor (@sensors) {
    my $h = $data->{$station}->{$sensor}->{'version'};
    for my $ts (keys %$h) {
      my $delta = $h->{$ts} - $ts;
      if($delta > 10*$HOUR) {
	print "$station $sensor $delta " . scalar(localtime($ts)) . "\n";
      }
    }
  }
}


