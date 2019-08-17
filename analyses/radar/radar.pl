#!/usr/bin/perl -w

# Given a set of directional readings of a pullutant, construct a PDF of the source
#

# Algorithm:
#  Fix the reading sensor and its location. Given a reading and the wind direction, compute for any point in space P,
#  the likelihood of it being the source for the reading. The pollutant is carried downwind, at an intensity growing inversely proportional to distance squared.
#  Using the wind direction, the bearing from source to sensor, and the reading intensity, work back the hypothetical source intensity.
#  Use a prior on intensity strength distribution to derive a likelihood estimate for the real source being at P.
#  Aggregate over all readings to derive a density for each P.



# Representation:
# The space S is a grid, with lower-left corner (X0, Y0) (X is longitude, Y is latitude), spanning NPx by NPy cells. Cell dimensions, in degrees, are CWx and CWy respectively.
# Each cell contains a list ref to all of the per-reading estimates made for this point.
#

use Data::Dumper;
use radarlib;
use Math::Trig;
use Getopt::Std;

my %loc = (			# station locations
	   'Igud' => {'lat'=>32.789167, 'lon'=>35.040556},
	   'K. Hayim- Regavim' => {'lat'=>32.831111, 'lon'=>35.054444},
	   'K. Binyamin' => {'lat' => 32.78866, 'lon' => 35.08511}
	  );

my %xlat_pollutant = ( 'TOL' => 'TOLUEN' );

getopts("p:", \%options);

my $pollutant = $options{'p'} || 'TOLUEN';

my $NPlon = 100;
my $NPlat = 100;
my ($minlat, $minlon, $maxlat, $maxlon) = (32.74, 35.0, 32.86, 35.15);

my $S = gen_grid($minlat, $minlon, $maxlat, $maxlon, $NPlon, $NPlat);

#for my $fname (qw/igud-wind-sep.csv/) {
#for my $fname (qw/regavim-wind-sep.csv/) {
#for my $fname (qw/binyamin-wind-sep.csv/) {
for my $fname (@ARGV) {
  my ($station, $dat) = read_data($fname, $pollutant);

  my $ndat = scalar(@{$dat->{'wind_dir'}});
  my $station_loc = $loc{$station};
  if(!defined($station_loc)) {
    die "Uknown station $station";
  }
  for($n=0; $n<$ndat; $n++) {
    my $wind_dir = $dat->{'wind_dir'}->[$n];
    my $reading = $dat->{$pollutant}->[$n];
    my $wind_speed = $dat->{'wind_speed'}->[$n];
    die if(!defined($wind_dir));
    apply_reading($S, $wind_dir, $wind_speed, $reading, $station_loc);
  }
}
exit;

dump_grid($S);

sub read_data {
  my ($fname, $pollutant) = @_;
  open(F, $fname) or die "$!: $fname";
  my %colnames;			# map from column names to index
  my $station;			# station name
  my %data = ('wind_dir' => [], 'wind_speed' => [], $pollutant => []);
  while(<F>) {
    chomp;
    my @line = split(/\s*,/, $_);
    if(!%colnames) {	# first row
      map {
	my $idx = $_;
	my $name = blacken($line[$_]);
	if(exists($xlat_pollutant{$name})) {
	  $name = $xlat_pollutant{$name};
	}
	$colnames{$name} = $idx;
      } 0..$#line;
      $station = $line[0];
    } else {
      my $wind_dir = blacken($line[$colnames{'WDD'}]);
      my $wind_speed = blacken($line[$colnames{'WDS'}]);
      my $val = blacken($line[$colnames{$pollutant}]);
      if(defined($wind_dir) && defined($val) && $wind_dir ne '' && $val ne '') {
	push(@{$data{'wind_dir'}}, $wind_dir);
	push(@{$data{'wind_speed'}}, $wind_speed);
	push(@{$data{$pollutant}}, $val);
      }
    }
  }
  close(F);
  return ($station, \%data);
}

sub blacken {
  my ($s) = @_;
  $s =~ s/^\s*//;
  $s =~ s/\s*$//;
  return $s;
}
