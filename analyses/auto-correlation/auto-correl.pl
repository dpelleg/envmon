#!/usr/bin/perl -w

# Given a day of observations, find historical days where the observations correlate highly

use envlib;

#my $station = "Nesher";
#my $station = "Kiryat Haim-Degania";
my $period_start = "20/10/2016 10:00";
my $period_hours = 8;

my $HOUR = 3600;

my @l1 = 1..10;
my @l2 = (11..15,5,3,2,5,-9);
# print "mse " . mse(\@l1, \@l2) . "\n"; exit;

my $curr_data = slurp_subdir_multistation("envdata/2016/10/");#, $period_hours * $HOUR + string_to_timestamp($period_start) + 24*$HOUR);
my $hist_data_ = slurp_subdir_multistation("envdata_hist/");

# compute list of timestamps for the inspected period
my $period_start_ts = string_to_timestamp($period_start);
my @ts_list = map { $period_start_ts + $HOUR*$_ } 0..($period_hours-1);

my @stations = keys %$curr_data;

for my $station (@stations) {

  my @sensors = keys %{$curr_data->{$station}};

  for my $sensor (@sensors) {
    my $ref_data = get_ref_data($curr_data, $station, $sensor, \@ts_list);

    if((sum($ref_data) != 0) && !all_identical($ref_data)) {

      my ($hist_data, $hist_data_start, $hist_data_end) = get_hist_data($hist_data_, $station, $sensor);

      # walk the historical data, try to find matches
      my ($best_corr, $best_corr_start) = (1e6, undef);
      for(my $now = $hist_data_start; $now + ($period_hours-1)*$HOUR <= $hist_data_end; $now += $HOUR) {
	my $window = get_reads_from($hist_data, $now, $period_hours);
	my $corr = mse($ref_data, $window);
	if($corr < $best_corr) {
	  $best_corr = $corr;
	  $best_corr_start = $now;
	}
      }

      if($best_corr < 0.1) {
	print "Ref $station $sensor:\nValues: " . join(", ", map {defnull($_) } @$ref_data) . "\n";
	my $best_window = get_reads_from($hist_data, $best_corr_start, $period_hours);
	print "Values: " . join(", ", map {defnull($_) } @$best_window) . "\n";
	printf("Best corr is %.2f, starting at %s (%d)\n\n", $best_corr, scalar(localtime($best_corr_start)), $best_corr_start);
      }
    }
  }
}

# retrieve a list of values starting at at given timestamp and extending the given #hours
sub get_reads_from {
  my ($dat, $start_ts, $duration) = @_;
  my @ret;
  my $ts = $start_ts;
  for(my $h=0; $h<$duration; $h++) {
    my $v;
    if(exists($dat->{$ts})) {
      $v = $dat->{$ts};
    }
    push(@ret, $v);
    $ts += $HOUR;
  }
  return \@ret;
}

sub get_ref_data {
  my ($dat, $station, $sensor, $ts_list) = @_;
  my @ret;
  my $hist_slice = $dat->{$station}->{$sensor}->{"reads"};
  for my $ts (@$ts_list) {
    my $v;
    if(exists($dat->{$station}->{$sensor}->{"reads"}->{$ts})) {
      $v = $dat->{$station}->{$sensor}->{"reads"}->{$ts};
    }
    push(@ret, $v);
  }
  return \@ret;
}

sub get_hist_data {
  my ($dat, $station, $sensor) = @_;
  my $ret_ts = $dat->{$station}->{$sensor}->{"reads"};
  my @ts_sorted = sort {$a <=> $b } keys %$ret_ts;
  my $ret_ts_start = shift(@ts_sorted);
  my $ret_ts_end = pop(@ts_sorted);
  return ($ret_ts, $ret_ts_start, $ret_ts_end);
}



sub pearson {
  my ($x_, $y_) = @_;
  my @x = map {def0($_)} @$x_;
  my @y = map {def0($_)} @$y_;

  die unless(scalar(@x) == scalar(@y));
  my $n = scalar(@x);

  my $sumx = sum(\@x);
  my $meanx = $sumx/scalar(@x);
  my $sumy = sum(\@y);
  my $meany = $sumy/scalar(@y);

  my $sumx_sqd = sum([map { $_*$_ } @x ]);
  my $sumy_sqd = sum([map { $_*$_ } @y ]);

  my $sum_prod = sum([map { $x[$_] * $y[$_] } 0..(scalar(@x)-1) ]);

  my $num = ($n * $sum_prod) - ($sumx * $sumy);
  my $den = (safe_sqrt($n * $sumx_sqd - $sumx * $sumx)) * (safe_sqrt($n * $sumy_sqd - $sumy * $sumy));

  my $ret = 0;
  if($den > 0) {
    $ret = $num/$den;
  }
  return $ret;
}

sub mse {
  my ($x_, $y_) = @_;
  my @x = map {def0($_)} @$x_;
  my @y = map {def0($_)} @$y_;

  die unless(scalar(@x) == scalar(@y));
  my $n = scalar(@x);

  my $ret = sum([ map { ($x[$_] - $y[$_]) ** 2 } 0..(scalar(@x)-1) ]);
  $ret /= $n;

  return $ret;
}

sub sum {
  my ($ss) = @_;
  my $ret = 0;
  map {
    $ret += $_ if(defined($_));
  } @$ss;
  return $ret;
}

sub all_identical {
  my ($ss) = @_;
  my $n = scalar(@$ss);
  return 1 if($n == 0);
  my $v = def0($ss->[0]);
  for(my $i=1; $i<$n; $i++) {
    return 0 if(def0($ss->[$i]) != $v);
  }
  return 1;
}

sub def0 {
  my ($s) = @_;
  return defined($s) ? $s : 0;
}

sub defnull {
  my ($s) = @_;
  return defined($s) ? $s : "null";
}

sub safe_sqrt {
  my ($v) = @_;
  my $ret = undef;
  eval { $ret = sqrt($v); };
  return $ret;
}
