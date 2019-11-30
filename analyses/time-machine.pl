#!/usr/local/bin/perl5.24.3 -w

# For a given day, and station/sensor, show the measurements as they were reported throughout the day

use envlib;
use Data::Dumper;
use Getopt::Std;
use DateTime;
use File::Find;

my $data_root = 'envdata';

getopts("d:s:n:", \%options);
if(!exists($options{'d'}) || !exists($options{'s'}) || !exists($options{'n'})) {
  die("Usage: $0 -d DATE -s STATION -n SENSOR");
}

my $date = $options{'d'};
my $station = $options{'s'};
my $sensor = $options{'n'};

my $dt;

if($date =~ m!^(\d{2})/(\d{2})/(\d{4})$!) { 
  my ($day, $month, $year) = ($1, $2, $3);
  $dt = DateTime->new(
    year       => $year,
    month      => $month,
    day        => $day,
      );
} else {
  die "Date format: day/month/year";
}

# read data for this particular day
my $datadir = sprintf("%s/%s", $data_root, $dt->ymd('/'));

my $dat;                        # per-dump entries, keyed by the timestamp of the file

eval {
  find({ wanted =>
             sub{
               my $fname = $_;
               my $ts = 0;
               if($fname =~ m!.*/(\d+)\.html(\.gz)?$!) {
                 $ts = $1;
               }
               print STDERR "reading $fname ($ts)\n";
               my $d = slurp_file_multistation($fname);
               $dat{$ts} =$d;
         },
         no_chdir => 1},
       $datadir);
}; warn $@ if $@;


# prepare output: hash of lines, keyed by the timestamp of the observation
my %out;
while( my($obs_ts, $vals) = each(%dat)) {
  my $h = $vals->{$station}->{$sensor};
  my @reads = sort keys %{$h->{'reads'}};
  my @by_hour;                  # stores the values per hour on the given date, 0..23
  for my $ts (@reads) {
    my $v = $h->{'reads'}->{$ts};
    my $read_ts = DateTime->from_epoch(epoch => $ts, time_zone => 'Asia/Jerusalem');
    my $hour = $read_ts->hour;
    if($read_ts->year == $dt->year &&
       $read_ts->month == $dt->month &&
       $read_ts->day == $dt->day) {
      $by_hour[$read_ts->hour] = $v;

      # prepare line of output
      my $obs_ts_dt = DateTime->from_epoch(epoch => $obs_ts, time_zone => 'Asia/Jerusalem');
      my $line = join(",",
                      $station,
                      $sensor,
                      $obs_ts_dt->ymd(''),
                      $obs_ts_dt->hour,
                      map { defz($by_hour[$_]) } 0..23);
      $out{$obs_ts} = $line;
    } else {
      #warn "mismatch: " . $read_ts->ymd('') . " !=! " . $dt->ymd(''); 
    }
  }
}


# sort
my @sorted = sort { $a <=> $b } keys %out;

# header
print(join(",",
           'station',
           'sensor',
           'observation_hour',
           'observation_date',
           map { "value at $_" } 0..23) . "\n");

# data
map {
  print $out{$_} . "\n";
} @sorted;


sub defz {
  my ($s) = @_;
  return defined($s) ? $s : "";
}
