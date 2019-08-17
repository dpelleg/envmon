#!/usr/bin/perl -w

# Given a log of observations, compute, for the "state" sensors, how long are they in some fault condition

use envlib;
use Data::Dumper;
use List::Util qw/sum/;

my @skip_sensors_state = (         #                   Encode as station:sensor
                                   'OP-BZN1:STAT[1-5]',
                                   'OP-GADIV:STAT[12]',
                                   'OP-BZN1:OPB31-Stat',
    );

my $skip_sensors_state = '^(' . join("|", @skip_sensors_state) . ')$';

# keyed by date, and station:sensor, stores a list of lag values in seconds
# at post-processing, each list is replaced by its mean
my %lags;


my $data = slurp_subdir_multistation("envdata/2016");
my %all_names;

for my $station (keys %$data) {
  my @sensors = keys %{$data->{$station}};
  for my $sensor (@sensors) {
    my $key = "$station:$sensor";
    if(($key !~ /$skip_sensors_state/) &&
       ($sensor =~ m/stat/i || $sensor =~ m/^((SPL|PRX|XLN|TLN)HT|CU\.[134]|Vis3\.|FCC|HVGO HSD|KERO HDS|Benzine HDS|CCR|GO HDS|Isomer|B11|B21|Boiler31)$/)) {
      my $h = $data->{$station}->{$sensor}->{'reads'};
      for my $ts (keys %$h) {
	my $dt_now = DateTime->from_epoch(epoch => $ts, time_zone => $envlib::TZ_HERE);
	my $day = $dt_now->ymd('');
	my $v = $h->{$ts};
	my $bad = (($v == 1) || ($v == 3)) ? 0 : 1;
	push(@{$lags{$day}->{$key}}, $bad);
	$all_names{$key} = 1;
      }
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
    push(@v, defined($vv) ? sprintf("%.2f", $vv) : "");
  }
  print join("\t", @v) . "\n";
}
