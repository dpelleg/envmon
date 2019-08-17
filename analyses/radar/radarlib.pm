package radarlib;

use Data::Dumper;
use Math::Trig;
use List::Util qw/sum/;
use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

# Model source strength as log-normal distribution - see https://en.wikipedia.org/wiki/Log-normal_distribution
my $lognormal_scale = 1;
my $lognormal_mean = 2;
my $lognormal_location = log($lognormal_mean) - ($lognormal_scale**2)/2;

$VERSION     = 1.00;
@ISA         = qw(Exporter);
@EXPORT      = (qw/distance bearing gen_grid apply_reading dump_grid/);

sub gen_grid {
  my ($minlat, $minlon, $maxlat, $maxlon, $NPlon, $NPlat) = @_;
  my %S = ('minlat' => $minlat,
	   'minlon' => $minlon,
	   'maxlat' => $maxlat,
	   'maxlon' => $maxlon,
	   'NPlat' => $NPlat,
	   'NPlon' => $NPlon,
	   'CWlon' => ($maxlon - $minlon)/$NPlon,
	   'CWlat' => ($maxlat - $minlat)/$NPlat,
	   'grid' => [],	# ref to array along the longitude axis, each cell is an array ref indexed by the latitude axis, holding a list ref to the estimates
	  );
  return \%S;
}

sub add_estimate {
  my ($S, $lat_idx, $lon_idx, $est) = @_;
  push(@{$S->{'grid'}->[$lon_idx]->[$lat_idx]}, $est);
}

sub get_loc {
  my ($S, $latidx, $lonidx) = @_;
  my %ret = (
	     'lat' => $S->{'minlat'} + ($latidx*$S->{'CWlat'}),
	     'lon' => $S->{'minlon'} + ($lonidx*$S->{'CWlon'})
	     );
  return \%ret;
}

sub apply_reading {
  my ($S, $wind_dir_deg, $wind_speed, $reading, $station_loc) = @_;
  if($wind_speed < 0.4 || $wind_speed > 2) { # wind too slow - no dispersion; wind too fast - dilution
    return;
  }
  for(my $lon_idx=0; $lon_idx<$S->{'NPlon'}; $lon_idx++) {
    for(my $lat_idx=0; $lat_idx<$S->{'NPlat'}; $lat_idx++) {
      my $cell_loc = get_loc($S, $lat_idx, $lon_idx);
      my $dist = distance($cell_loc, $station_loc);
      my $bearing = bearing($cell_loc, $station_loc);
      my $wind_dir_rad = deg2rad($wind_dir_deg);
      my $downwind_dir = wrap2pi($bearing - $wind_dir_rad);

      # print STDERR sprintf("wd_rad %f downwind %f\n", $wind_dir_rad, $downwind_dir);
      if(abs($downwind_dir) < pi/2) { # if downwind direction is 90 degrees or more, there is no effect
	if($reading > 0) { # if reading is non-positive, there is no effect
	  #
	  # reading at station is concentration at origin C, times cosine of downwind direction, divided by distance
	  # work back to get C
	  if($wind_speed <= 0) {
	    $wind_speed = 1e-3;
	  }
	  my $C = $reading * $wind_speed * cos($downwind_dir);

	  # use the prior on concetration distribution to work out the likelihood
	  #my $C_prior = concentration_pdf(1.0/$dist);
	  my $C_prior = 1.0;	# BUGBUG
	  #my $C_prior = $reading / cos($downwind_dir);

	  add_estimate($S, $lat_idx, $lon_idx, $C*$C_prior);
	}
      }
    }
  }
}

sub dump_grid {
  my ($S) = @_;
  my $g = $S->{'grid'};
  for(my $lat_idx=0; $lat_idx<$S->{'NPlat'}; $lat_idx++) {
    my @vals;
    for(my $lon_idx=0; $lon_idx<$S->{'NPlon'}; $lon_idx++) {
      my $v = '';
      if(defined($g->[$lon_idx]->[$lat_idx])) {
	my @list = @{$g->[$lon_idx]->[$lat_idx]};
	$v = sum(@list);
      }
      push(@vals, $v);
    }
    print join(",", @vals) . "\n";
  }
}

sub concentration_pdf {
  return lognormal_pdf(@_);
  #return uniform_pdf(@_);
}

sub uniform_pdf {
  my ($x) = @_;
  return 1;
}

sub lognormal_pdf {
  my ($x) = @_;
  my $num_num = (log($x) - $lognormal_location) ** 2;
  my $num_denom = 2* ($lognormal_scale**2);
  my $num = exp(- $num_num / $num_denom);
  my $denom = $x * $lognormal_scale * sqrt(2*pi);
  return $num/$denom;
}

# haversine distance, in KM
sub distance {
  my ($p1, $p2) = @_;
  my $RADIUS = 6372;		# earth's radius in KM

  my $lat_A = $p1->{'lat'};
  my $lon_A = $p1->{'lon'};
  my $lat_B = $p2->{'lat'};
  my $lon_B = $p2->{'lon'};

  my $distance = sin(deg2rad($lat_A))
      * sin(deg2rad($lat_B))
      + cos(deg2rad($lat_A))
      * cos(deg2rad($lat_B))
      * cos(deg2rad($lon_A - $lon_B));

  $distance = acos($distance) * $RADIUS;

  return $distance;
}

sub bearing {
  my ($p1, $p2) = @_;
  my $lat1 = deg2rad($p1->{'lat'});
  my $lon1 = deg2rad($p1->{'lon'});
  my $lat2 = deg2rad($p2->{'lat'});
  my $lon2 = deg2rad($p2->{'lon'});

  my $ret = atan2(sin($lon2-$lon1)*cos($lat2),
		cos($lat1)*sin($lat2)-sin($lat1)*cos($lat2)*cos($lon2-$lon1));

  return wrap2pi($ret);
}

# ensure proper wrapping to [0, 2pi]
sub wrap2pi {
  my ($d) = @_;
  return deg2rad(rad2deg($d));
}

1;

