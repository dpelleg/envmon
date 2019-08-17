#!/usr/bin/perl -w

# process the output of sensor-corr.R

my %xlat = ( 'שפרינצק' => 'Shprintzak',
             'רוממה' => 'Romema',
             'קריית ים' => 'Kiryat Yam',
             'קריית טבעון' => 'Kiryat Tivon',
             'קריית חיים-דגניה' => 'Kiryat Haim-Degania',
             'קריית ביאליק' => 'Kiryat Bialik',
             'קריית אתא' => 'Kiryat Ata',
             'נשר' => 'Nesher',
             'נווה שאנן' => 'Neve Shaanan',
             'נווה יוסף' => 'Neve Yosef',
             'כרמליה' => 'Carmelia',
             'כפר חסידים' => 'Kfar Hasidim',
             'יזרעאליה' => 'Yizraelia',
             'איינשטיין' => 'Einstein',
             'איגוד' => 'Igud (check-post)',
             'אחוזה' => 'Ahuza',
             'קריית מוצקין' => 'Kiryat Motzkin',
             'קריית בנימין' => 'Kiryat Binyamin',
             'דליית אל כרמל' => 'D.CARMEL',
	   );

my %dest = map { $xlat{$_} => 1 } keys %xlat;

while(<ARGV>) {
  chomp;
  next if(/^"ind.row/);		# header row
  my ($ind, $row, $col, $rowname, $colname) = split(/\t/, $_);
  my ($station_row, $sensor_row) = splitname($rowname);
  my ($station_col, $sensor_col) = splitname($colname);
  next if($station_col eq $station_row); # correlation within station - not interesting
  my $col_city = exists($dest{$station_col});
  my $row_city = exists($dest{$station_row});
  next if($row_city && $col_city); # both stations are pollutant destination (in the city) - not interesting
  next if(!$row_city && !$col_city); # neither station is pollutant destination (in the city) - not interesting
  next if($sensor_row =~ /^(WIND|WD|StWd|TEMP)/ && $sensor_col =~ /^(WIND|WD|StWd|TEMP)/); # both sensors are climate sensors
  next if($station_row =~ /^Gadiv Nox emission rate/ && $station_col =~ /^bazan NOx emission rate/);
  next if($station_row eq 'Mobile_New');
  next if($station_col eq 'Mobile_New');
  print "$rowname $colname\n";
}


sub splitname {
  my ($s) = @_;
  $s =~ s/^"//;
  $s =~ s/"$//;
  my ($station, $sensor) = split(/\//, $s);
  return ($station, $sensor);
}
