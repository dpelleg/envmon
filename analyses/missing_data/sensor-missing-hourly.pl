#!/usr/bin/perl -w

# Report on the availability of data, as number of hours missing data in each date

use DateTime;
use Data::Dumper;
use envlib;
use Getopt::Std;

my %options;
getopts('N', \%options);

my $year = 2016;

my $skip_stations = 'AAAAANONE'; #'^(Mobile_New|Haifa g-[34]0|mahzam [34]0)$';

my %sensor_conv = (
		   'B11' => 'Scada',
		   'B201B CU3' => 'Scada',
		   'B21' => 'Scada',
		   'BP' => 'Climate',
		   'Benzene-Namal' => 'BENZN',
		   'Benzine HDS' => 'BENZN',
		   'Boil-start' => 'Scada',
		   'Boiler1-Stat' => 'Scada',
		   'Boiler2-Stat' => 'Scada',
		   'Boiler3-Stat' => 'Scada',
		   'Boiler31' => 'Scada',
		   'CCR' => 'Scada',
		   'CO TRAFIC' => 'CO',
		   'CONC' => 'Scada',
		   'CO_I' => 'Scada',
		   'CU.1' => 'Scada',
		   'CU.3' => 'Scada',
		   'CU.4' => 'Scada',
		   'DUST_I' => 'Scada',
		   'FCC' => 'Scada',
		   'Filter' => 'Scada',
		   'FILTER-2.5' => 'Scada',
		   'Filter_I' => 'Scada',
		   'GO HDS' => 'Scada',
		   'HCU Stat' => 'Scada',
		   'HVGO HSD' => 'Scada',
		   'ISO' => 'Scada',
		   'ITemp' => 'Scada',
		   'Isomer' => 'Scada',
		   'KERO HDS' => 'Scada',
		   'LAF_max' => 'Scada',
		   'LAF_min' => 'Scada',
		   'LAeq' => 'Scada',
		   'LAim' => 'Scada',
		   'LXeq' => 'Scada',
		   'LXpk_max' => 'Scada',
		   'N1 Status' => 'Scada',
		   'N2 Status' => 'Scada',
		   'NO TRAFIC' => 'NO',
		   'NO2 TRAFIC' => 'NO2',
		   'NO2_I' => 'NO2',
		   'NOB1' => 'Scada',
		   'NOB2' => 'Scada',
		   'NOB3' => 'Scada',
		   'NOX TRAFIC' => 'NOX',
		   'NOX_I' => 'NOX',
		   'NO_I' => 'NO',
		   'NOx Dry' => 'NOX',
		   'Nox - N2' => 'NOX',
		   'Nox N1' => 'NOX',
		   'O3_I' => 'O3',
		   'OP-HCU' => 'Scada',
		   'OPB31' => 'Scada',
		   'OPB31-Stat' => 'Scada',
		   'OPCB1' => 'Scada',
		   'OPCB2' => 'Scada',
		   'OPCB3' => 'Scada',
		   'OP_HPU' => 'Scada',
		   'Opa11' => 'Scada',
		   'Opa21' => 'Scada',
		   'OpaC1' => 'Scada',
		   'OpaC3' => 'Scada',
		   'OpaC4' => 'Scada',
		   'OpaCR' => 'Scada',
		   'OpaFC' => 'Scada',
		   'OpaGH' => 'Scada',
		   'OpaHD' => 'Scada',
		   'OpaV3' => 'Scada',
		   'PREC' => 'Climate',
		   'PRX-start' => 'Scada',
		   'PRXHT' => 'Scada',
		   'RH' => 'Climate',
		   'RTO' => 'Scada',
		   'SO2FC' => 'SO2',
		   'SO2S3' => 'SO2',
		   'SO2S4' => 'SO2',
		   'SO2_I' => 'SO2',
		   'SPLHT' => 'Scada',
		   'SPL_start' => 'Scada',
		   'SR' => 'Climate',
		   'STAT1' => 'Scada',
		   'STAT2' => 'Scada',
		   'STAT3' => 'Scada',
		   'STAT5' => 'Scada',
		   'STBLR' => 'Scada',
		   'StWd' => 'Climate',
		   'Stack_NOx' => 'NOX',
		   'Stat-HCU' => 'Scada',
		   'Stat-HPU' => 'Scada',
		   'Status' => 'Scada',
		   'TEMP' => 'Climate',
		   'TO' => 'TOC',
		   'TOC_RTO' => 'TOC',
		   'TOC_TO' => 'TOC',
		   'TOL' => 'TOLUEN',
		   'TOL-start' => 'Scada',
		   'Vis3.' => 'Scada',
		   'WDD' => 'Climate',
		   'WDS' => 'Climate',
		   'XYL-start' => 'Scada',
		   'activity and goals' => 'Scada',
		   'cu1 B4' => 'Scada',
		   'TLNHT' => 'Scada',
		   'XLNHT' => 'Scada',
		   );

my $climate_sensors = '^(RH|BP|WDS|TEMP|WDD|SR|PREC|StWd|WIND-V6|ITemp|FILTER.*|Filter|WIND-DIR)$';

# load data and compute events

my $dat = slurp_subdir_multistation("envdata_hist/${year}/");

# list ref of dates to check, ordered
my $dates;

# helper map from dates in string format, to indices in @dates
my $dates_idx;

($dates, $dates_idx) = all_dates($year);
my $cdates = $#{$dates};

# hash of station->sensor->list of available data in each date in @dates
my $obs;

# header
print join("\t", qw/station sensor/, @$dates,
	     		      ) . "\n";
$obs = find_missing($dat, $dates_idx, $obs);

for my $station (sort keys %$obs) {
  my $station_v = $obs->{$station};
  for my $sensor (sort keys %$station_v) {
    my @out = ($station, $sensor);
    my $counts = $station_v->{$sensor};
    push(@out, map { def0($counts->[$_]) } 0..$cdates);
    print join("\t", @out) . "\n";
  }
}

sub def0 {
  my ($s) = @_;
  return defined($s) ? $s : 0;
}

sub find_missing {
  my ($dat, $dates_idx, $events) = @_;
  my %warned;
  
  $events = {} unless defined($events);
 
  for my $station (keys %$dat) {
    my $station_v = $dat->{$station};
    next if($station =~ /$skip_stations/);
    for my $sensor (keys %$station_v) {
      next if($sensor =~ /$climate_sensors/);
        my $vals = $station_v->{$sensor}->{'reads'};
        for my $ts (keys %$vals) {
	  next if($vals->{$ts} < 0); # negative values don't count
          my $dt = DateTime->from_epoch(epoch => $ts);
	  my $day = $dt->ymd('');
	  my $date_idx = $dates_idx->{$day};
	  if(defined($date_idx)) {
	    $events->{$station}->{$sensor}->[$date_idx]++;
	  } else {
	    if(!$warned{$day}) {
	      warn "date $day";
	    }
	    $warned{$day} = 1;
	  }
        }
      }
  }

  return $events;
}

# compute a list of all dates in a year,
sub all_dates {
  my ($year) = @_;

  my @all_dates;

  my $dt = DateTime->new(year => $year, month => 1, day => 1);
  
  while($dt->month() < 11) {
    push(@all_dates, $dt->ymd(''));
    $dt->add(days => 1);
  }

  # as a hash to indices
  my %all;
  map {
    my $idx = $_;
    $all{$all_dates[$idx]} = $idx;
  } 0..$#all_dates;

  return (\@all_dates, \%all);
}
