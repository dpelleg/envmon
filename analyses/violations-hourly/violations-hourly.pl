#!/usr/bin/perl -w

# parse the HTML table read by fetch-envhaifa.pl, output the violations from the hourly targets

use Data::Dumper;
use envlib;

my %warn_levels_legal_hourly = ( # from:
				# from http://www.envihaifa.net/APP_Files/%D7%98%D7%91%D7%9C%D7%AA%20%D7%9E%D7%99%D7%93%D7%A2%20%D7%9C%D7%A0%D7%AA%D7%95%D7%A0%D7%99%20%D7%A4%D7%9C%D7%99%D7%98%D7%95%D7%AA%20%D7%9E%D7%9E%D7%A4%D7%A2%D7%9C%D7%99%D7%9D/%D7%98%D7%91%D7%9C%D7%AA%20%D7%9E%D7%99%D7%93%D7%A2%20%D7%9C%D7%A0%D7%AA%D7%95%D7%A0%D7%99%20%D7%A4%D7%9C%D7%99%D7%98%D7%95%D7%AA%20%D7%9E%D7%9E%D7%A4%D7%A2%D7%9C%D7%99%D7%9D.pdf
				'BAZAN HCU:NOx Dry' => [150],
				'BAZAN TO-1,2:TOC' => [20],
				'BAZAN TO-4:TOC' => [40],
				'BAZAN VRU:CONC' => [1000],    # stack #99163
				'BAZAN_HDS:NOx Dry' => [150],        # stack #9755
				'BAZAN_HPU:NOx Dry' => [120],
				'BAZAN-ISOM:NOx Dry' => [150],	 # stack # 62818
				'Bazan-VIS3:NOx Dry' => [150],	 # stack # 9749
				'BAZAN6-SO2:SO2FC' => [600],	 # stack # 9753
				'BAZAN6-SO2:SO2S4' => [200],
				'BAZAN6-SO2:SO2S3' => [200],	 # stack # 9741
				'BAZAN-B11:Stack_NOx' => [100],	 # stack # 9735
				'BAZAN-B21:NOx Dry' => [100],	 # stack # 9737
				'BAZAN-B31:Stack_NOx' => [120],	 # stack # 136992
				'BAZAN-CCR:NOx Dry' => [150],	 # stack # 9751
				'Bazan-CU1:NOx Dry' => [150],	 # stack # 9747
				'Bazan-CU1-B4:NOx Dry' => [150],	 # stack # 137015
				'Bazan-CU3:NOx Dry' => [150],	 # stack # 9745
				'Bazan-CU3-B201B:NOx Dry' => [150],	 # stack # 137056
				'BAZAN-CU4:Stack_NOx' => [150],	 # stack # 9743
				'BAZAN-FCC:NOx Dry' => [350],	 # stack # 9753
				'BAZAN-IHDS-GO:NOx Dry' => [150],	 # stack # 40475
				'CAOL RTO:TOC' => [20],	 # stack # 164786
				'CAOL_New:NOB1' => [200],	 # stack # 10712
				'CAOL_New:NOB2' => [200],	 # stack # 10813
				'CAOL_New:NOB3' => [200],	 # stack # 10815
				'GADIV CTO:BENZN' => [5],	 # stack # 176967
				'Gadiv new:Benzene-Namal' => [2000],
				'Gadiv-AS2XYL1:NOx Dry' => [300],	 # stack # 8807
				'Gadiv-Boiler:NOx Dry' => [300],	 # stack # 8815
				'Gadiv-PAREX:NOx Dry' => [200],	 # stack # 8811
				'Gadiv-TOL:NOx Dry' => [200],	 # stack # 8809
				'Gadiv-XYl2:NOx Dry' => [200],	 # stack # 8813
				'HaifaChem:Nox N1' => [300],
				'HaifaChem:Nox - N2' => [300],
				'Deshanim:NOX' => [300],
				'mahzam 30:NOX' => [100],	 # stack # 163134
				'mahzam 40:NOX' => [100],	 # stack # 128377
				'TARO:TOC' => [10],
				'Dor chemicals:formaldehyde' => [2],
				'Delek:TOC' => [2000],
				'Sonol:TOC' => [2000],
				'Paz:TOC' => [2000],
				'Paz Shmanim:TOC' => [40],
				'Shemen:TOC' => [100],
				'Tashan fuel port:TOC' => [300],
			       );

print join(",", qw/station sensor reading threshold date year month month_day hour week_day year_day/
	     		      ) . "\n";
#my @flist = qw/1478304600.html 1478308200.html 1478311800.html 1478315400.html 1478319000.html 1478322600.html 1478326200.html 1478329800.html 1478333400.html 1478337001.html 1478340600.html 1478344200.html 1478347800.html 1478351400.html 1478355000.html 1478358600.html 1478362200.html 1478365800.html 1478369400.html 1478373000.html 1478376600.html 1478380200.html 1478383800.html 1478387400.html/;

my $data = slurp_subdir_multistation("envdata_hist/2017");


#for my $f (@flist) {

while (my ($key, $value) = each %warn_levels_legal_hourly) {
  if($key =~ /^(.+):(.+)$/) {
    my ($station, $sensor) = ($1, $2);
    print STDERR "$station, $sensor\n";
    my $threshold = $value->[0];
    my $slice = $data->{$station};
    my $h = $slice->{$sensor}->{'reads'};
    map {
      my $ts = $_;
      my $v = $h->{$ts};
      if($v > $threshold) {
	my $today = DateTime->from_epoch(epoch => $ts, time_zone => $envlib::TZ_HERE);
	print join(",", $station, $sensor,
		   $v,
		   $threshold,
		   $today->ymd(''),
		   $today->year,
		   $today->month,
		   $today->day,
		   $today->hour,
		   $today->day_of_week,
		   $today->day_of_year,
		  ) . "\n";
      }
    } sort keys %$h;
  }
}
