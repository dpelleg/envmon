#!/usr/bin/perl -w

# create an HTML table representing annual calendar(s), each cell is a value corresponding to a sensor reading, or to multiple readings
# To get fresh data to run on, use "parse-semester.pl" (which in turn requires to run fetch-semester.pl )

use DateTime;
use Storable qw/lock_store lock_retrieve/;
use Data::Dumper;

#my $OUTPUT = 'HTML';
my $OUTPUT = 'CSV';

my $year_start = 2010;
my $year_end = 2017;

my $max_rect_width = 100;

my $min_sensor_gap = 100*24*60*60;

my $skip_stations = '^(AAAAAAA_NONE)$';
#my $skip_stations = '^(CAOL|TARO|Gadiv|Deshanim)';

my %legend;			# map events to colors

my %legal_mean_max_daily = (
			    'SO2' => 50,
			    'TOLUEN' => 3770,
			    'formaldehyde' => 0.8, # This is in microgram/m^3, while the IGUD report is said to be at miligram/m^3 - I think IGUD is really microgram
			    '1,3 Butadyen' => 0.11,
			    'BENZN' => 3.9,
			    'PM10' => 130,
			    'PM2.5' => 37.5,
			    'NO2' => 200,   # for one hour
			    # from http://www.envihaifa.net/APP_Files/%D7%98%D7%91%D7%9C%D7%AA%20%D7%9E%D7%99%D7%93%D7%A2%20%D7%9C%D7%A0%D7%AA%D7%95%D7%A0%D7%99%20%D7%A4%D7%9C%D7%99%D7%98%D7%95%D7%AA%20%D7%9E%D7%9E%D7%A4%D7%A2%D7%9C%D7%99%D7%9D/%D7%98%D7%91%D7%9C%D7%AA%20%D7%9E%D7%99%D7%93%D7%A2%20%D7%9C%D7%A0%D7%AA%D7%95%D7%A0%D7%99%20%D7%A4%D7%9C%D7%99%D7%98%D7%95%D7%AA%20%D7%9E%D7%9E%D7%A4%D7%A2%D7%9C%D7%99%D7%9D.pdf
			    'BAZAN HCU:NOx Dry' => 100,
			    'BAZAN TO-1,2:TOC' => 10,
			    'BAZAN TO-4:TOC' => 20,
			    'BAZAN VRU:CONC' => 500,    # stack #99163
			    'BAZAN_HDS:NOx Dry' => 100,        # stack #9755
			    'BAZAN_HPU:NOx Dry' => 100,
			    'BAZAN-ISOM:NOx Dry' => 100,	 # stack # 62818
			    'Bazan-VIS3:NOx Dry' => 100,	 # stack # 9749
			    'BAZAN6-SO2:SO2FC' => 300,	 # stack # 9753
			    'BAZAN6-SO2:SO2S4' => 100,
			    'BAZAN6-SO2:SO2S3' => 100,	 # stack # 9741
			    'BAZAN-B11:Stack_NOx' => 90,	 # stack # 9735
			    'BAZAN-B21:NOx Dry' => 90,	 # stack # 9737
			    'BAZAN-B31:Stack_NOx' => 100,	 # stack # 136992
			    'BAZAN-CCR:NOx Dry' => 100,	 # stack # 9751
			    'Bazan-CU1:NOx Dry' => 100,	 # stack # 9747
			    'Bazan-CU1-B4:NOx Dry' => 100,	 # stack # 137015
			    'Bazan-CU3:NOx Dry' => 100,	 # stack # 9745
			    'Bazan-CU3-B201B:NOx Dry' => 100,	 # stack # 137056
			    'BAZAN-CU4:Stack_NOx' => 100,	 # stack # 9743
			    'BAZAN-FCC:NOx Dry' => 300,	 # stack # 9753
			    'BAZAN-IHDS-GO:NOx Dry' => 100,	 # stack # 40475
			    'BAZAN-ISOM:NOx Dry' => 100,	 # stack # 62818
			    'CAOL RTO:TOC' => 10,	 # stack # 164786
			    'CAOL_New:NOB1' => 150,	 # stack # 10712
			    'CAOL_New:NOB2' => 150,	 # stack # 10813
			    'CAOL_New:NOB3' => 150,	 # stack # 10815
			    'GADIV CTO:BENZN 2.5' => 176967,
			    'Gadiv new:Benzene-Namal' => 1000,
			    'Gadiv-AS2XYL1:NOx Dry' => 150,	 # stack # 8807
			    'Gadiv-Boiler:NOx Dry' => 150,	 # stack # 8815
			    'Gadiv-PAREX:NOx Dry' => 100,	 # stack # 8811
			    'Gadiv-TOL:NOx Dry' => 100,	 # stack # 8809
			    'Gadiv-XYl2:NOx Dry' => 100,	 # stack # 8813
			    'HaifaChem:Nox N1' => 150,
			    'HaifaChem:Nox - N2' => 200,
			    'Deshanim:NOX' => 180,
			    'mahzam 30:NOX' => 50,	 # stack # 163134
			    'mahzam 40:NOX' => 50,	 # stack # 128377
			    'TARO:TOC' => 5,
			    'Dor chemicals:formaldehyde' => 1,
			    'Delek:TOC' => 1000,
			    'Sonol:TOC' => 1000,
			    'Paz:TOC' => 1000,
			    'Paz Shmanim:TOC' => 20,
			    'Shemen:TOC' => 50,
			    'Tashan fuel port:TOC' => 150,
			   );

# pallette of possible colors to use
# pallette of possible colors to use (from R: rainbow(5))
my @colors = (
	      "FF0000", "FF9900", "CCFF00", "33FF00", "00FF66", "00FFFF", "0066FF", "3300FF", "CC00FF", "FF0099",
	      );

my $last_color_used = -1;

# load data and compute events
my $dat = lock_retrieve('hist_daily.db');


#my $title = "PM 2.5 &gt; 25";
#my $events = find_exceed($dat, 'PM2.5', 25);

my ($title, $events);

if($OUTPUT eq 'CSV') {
print join("\t",
	   'date',
	   'year',
	   'month',
	   'day',
	   'day_of_week',
	   'day_of_year',
	   'station',
	   'sensor',
	   'value',
	   'threshold') . "\n";
}

if(0) {				# serious events, but daily values sometimes lowered
  $title = "PM2.5>25; PM10>50; NOX>30; NO2>200; BENZN>3.9; O3>100; SO2>20; TOL>3770; CO>100";

  $events = find_exceed($dat, 'PM2.5', 25, $events);
  $events = find_exceed($dat, 'PM10', 50, $events);
  $events = find_exceed($dat, 'NOX', 30, $events);
  $events = find_exceed($dat, 'NO2', 200, $events);
  $events = find_exceed($dat, 'BENZN', 3.9, $events);
  $events = find_exceed($dat, 'O3', 100, $events);
  $events = find_exceed($dat, 'SO2', 20, $events);
  $events = find_exceed($dat, 'TOL', 3770, $events);
  $events = find_exceed($dat, 'CO', 100, $events);
  #$events = transform_to_counts($events, 2, 2, 'exceeding PM2.5 value');

  #$events = find_missing($dat, $year_start, $year_end, $events);
} else { # clear violations of the HEITER PLITA
  $title = "Over HEITER PLITA";
  while (my ($key, $value) = each %legal_mean_max_daily) {
    if($key =~ /^(.+):(.+)$/) {
      my ($station, $sensor) = ($1, $2);
      next if($station =~ /$skip_stations/);
      $events = find_exceed_specific($dat, $station, $sensor, $value, $events, 1);
    }

  }
}

if($OUTPUT ne 'CSV') {
  my $y = "";
  for my $year ($year_start..$year_end) {
    $y .= annual_table($year, $events);
  }

  my $legend = "";
  for my $evtype (sort keys \%legend) {
    my $col = $legend{$evtype};
    $legend .= rect($col) . $evtype . "&nbsp;&nbsp;&nbsp;";
  };

  $legend .= "<br></br>\n";

  printf("<html><head></head><body>%s%s%s%s</body></html>\n", "<h2>EVENT: $title</h2>", $legend, $y, $legend);
}

sub annual_table {
  my ($year, $events) = @_;
  my $ret = "<H3>$year</H3>\n";
  my $td = "TD width=\"100\"";
  my $weekstr = "<$td ALIGN=center>Sun</TD><$td ALIGN=center>Mon</TD><$td ALIGN=center>Tue</TD><$td ALIGN=center>Wed</TD><$td ALIGN=center>Thu</TD><$td ALIGN=center>Fri</TD><$td ALIGN=center>Sat</TD>";
  $ret .= sprintf("%s\n<TR><TD></TD>%s</TR>\n", "<TABLE BORDER=1 CELLSPACING=0>",
		 $weekstr x 6);
  for my $month (1..12) {
    my $m = "<TR>";

    my $today = DateTime->new(year => $year, month => $month, day => 1);
    my $cells_done = 0;

    $m .= "<$td>" . $today->month_abbr() . "</TD>\n";

    # figure out the day of week of the first day of the month
    my $dow = $today->dow();
    # Monday-based system, transform to Sunday-based (both start at 1)
    $dow = ($dow % 7) + 1;

    # filler
    if($dow > 1) {
      $m .= sprintf("<TD colspan=%d></TD>\n", $dow - 1);
      $cells_done += $dow - 1;
    }

    # progress through the month, a day at a time
    while($today->month() == $month) {
      my $in_cell = "";
      my $key = $today->ymd('');
      if(exists($events->{$key})) {
	my $evs = $events->{$key};
	while(my ($evtype, $evcount) = each %$evs) {
	  my $color = get_color($evtype);
	  $in_cell .= rect("#" . $color, $evcount);
	}
	$in_cell .= $today->day();
      }

      $m .= sprintf("<$td>%s</TD>", $in_cell);
      $today->add(days => 1);
      $cells_done += 1;
    }

    # add a filler element till the end
    $m .= sprintf("<TD colspan=%d></TD>\n", 42 - $cells_done);
    
    $m .= "</TR>\n";
    $ret .= $m;
  }

  $ret .= "\n</TABLE>\n";
  return $ret;
}

sub rect {
  my ($color, $width) = @_;
  if(defined($width)) {
    $width *= 10;
  } else {
    $width = 30;
  }
  $width = $max_rect_width if($width > $max_rect_width);
  my $height = 5;
  my $el = 'div';
  my $ret = sprintf('<%s style="width:%dpx;height:%dpx;border:0px; background-color:%s;"></%s>', $el, $width, $height, $color, $el);
  return $ret;
}
    
sub get_color {
  my ($event_type) = @_;
  if(exists($legend{$event_type})) {
    return $legend{$event_type};
  }
  my $col_idx = ++$last_color_used % scalar(@colors);
  $legend{$event_type} = $colors[$col_idx];
  return $legend{$event_type};
}

sub find_exceed {
  my ($dat, $sensor, $threshold, $events) = @_;
  my $key = "$sensor &gt; $threshold";
  $events = {} unless defined($events);

  for my $station (keys %$dat) {
    next if($station =~ /$skip_stations/);
    if(exists($dat->{$station}->{$sensor})) {
      my $vals = $dat->{$station}->{$sensor}->{'reads'};
      for my $ts (keys %$vals) {
        my $v = $vals->{$ts};
        if($v > $threshold) {
          my $dt = DateTime->from_epoch(epoch => $ts);
	  if($OUTPUT eq 'CSV') {
	    print join("\t",
		       $dt->ymd(''),
		       $dt->year,
		       $dt->month,
		       $dt->day,
		       $dt->day_of_week,
		       $dt->day_of_year,
		       $station,
		       $sensor,
		       $v,
		       $threshold) . "\n";
	  }
          $events->{$dt->ymd('')}->{$key}++;
        }
      }
    }
  }
  return $events;
}

sub find_exceed_specific {
  my ($dat, $station, $sensor, $threshold, $events, $fudge_factor) = @_;
  $fudge_factor = 1 unless(defined($fudge_factor));
  my $key = "$station $sensor &gt; $threshold";
  $events = {} unless defined($events);

  if(exists($dat->{$station}->{$sensor})) {
    my $vals = $dat->{$station}->{$sensor}->{'reads'};
    for my $ts (keys %$vals) {
      my $v = $vals->{$ts};
      if("$station:$sensor" eq 'Gadiv-TOL:NOx Dry') { # DEBUG
	my $dt2 = DateTime->from_epoch(epoch => $ts);
	if($dt2->year == 2017 && $dt2->month == 3) {
	  print STDERR join(" ", $dt2->ymd(), $v, $threshold) . "\n";
	}
      }
      if($v > ($fudge_factor * $threshold)) {
	my $dt = DateTime->from_epoch(epoch => $ts);
	if($OUTPUT eq 'CSV') {
	  print join("\t",
		     $dt->ymd(''),
		     $dt->year,
		     $dt->month,
		     $dt->day,
		     $dt->day_of_week,
		     $dt->day_of_year,
		     $station,
		     $sensor,
		     $v,
		     $threshold) . "\n";
	}
	$events->{$dt->ymd('')}->{$key}++;
      }
    }
  }

  return $events;
}

sub find_missing {
  my ($dat, $year_start, $year_end, $events) = @_;
  $events = {} unless defined($events);
 
  for my $station (keys %$dat) {
    my $station_v = $dat->{$station};
    next if($station =~ /$skip_stations/);
    for my $sensor (keys %$station_v) {
        my $vals = $dat->{$station}->{$sensor}->{'reads'};
	my $all_dates = all_online_dates($vals);
        for my $ts (keys %$vals) {
          my $dt = DateTime->from_epoch(epoch => $ts);
          delete($all_dates->{$dt->ymd('')});
        }
        # put any remaining dates in master list
	my $key = "missing $sensor";
        for my $missing_date ( keys %$all_dates) {
          $events->{$missing_date}->{$key}++;
        }
      }
  }

  if(1) {
  # now transform ret into list format
  for my $date (keys %ret) {
    my $n_missing = scalar keys %{$ret{$date}};
    if (0) {
      if($n_missing >= 2) {
      $n_missing = "2+" if($n_missing >= 2);
      push(@{$events->{$date}}, sprintf("%s stations missing data", $n_missing));
    }
    } else {
      $events->{$date} = [ keys %{$ret{$date}} ];
    }
  }
}
  
  return $events;
}

sub transform_to_counts {
  my ($events, $min_value, $max_value, $name) = @_;

  $name = "missing data" unless defined($name);

  for my $date (keys %$events) {
    my $n = scalar @{$events->{$date}};
    if($n >= $min_value) {
      if($n >= $max_value) {
        $n = $max_value . "+";
      }
      $events->{$date} = [ sprintf("%s stations %s", $n, $name) ];
    } else {
      $events->{$date} = undef;
    }
  }
  return $events;
}

# compute a list of all dates in range, except those within a large gap
sub all_online_dates {
  my ($all_ts) = @_;

  my @all_dates;

  my @all_ts_sorted = sort {$a <=> $b } keys %$all_ts;
  my $prev_ts = shift(@all_ts_sorted);
  
  for my $ts (@all_ts_sorted) {
    my $gap = $ts - $prev_ts;
    if($gap < $min_sensor_gap) {
      my $dt = DateTime->from_epoch(epoch => $prev_ts);
      while($dt->epoch() <= $ts) {
	push(@all_dates, $dt->ymd(''));
	$dt->add(days => 1);
      }
    } else {
    }
    $prev_ts = $ts;
  }

  # as a hash:
  my %all = map { $_ => 1 } @all_dates;

  return \%all;
}
