#!/usr/local/bin/perl5.24.3 -w

use Storable qw/lock_store lock_retrieve/;
use POSIX;
use envlib;
use Time::Local;
use List::Util qw(first max maxstr min minstr reduce shuffle sum);
use Data::Dumper;
use Getopt::Std;
use File::Temp qw/ tempfile  /;

# maximum amount of history to consider
my $day = 24*3600;
my $year = 365 * $day;
my $history_lookback = 1.0*$year;

# maximum window of current sensor data to look back into
my $alert_lookback_hours = 24;

# Conversion factors from mg/m3 to PPB
# !!! this is at sea level !!!!
# !!! For other altitudes, numbers are different !!!
#
my %unit_coefficient = ( # from http://air.net.technion.ac.il/files/2014/10/%D7%A2%D7%A8%D7%9B%D7%99-%D7%A1%D7%91%D7%99%D7%91%D7%94-%D7%9C%D7%A0%D7%99%D7%98%D7%95%D7%A8-%D7%A8%D7%A6%D7%99%D7%A3-%D7%9B%D7%95%D7%9C%D7%9C-%D7%9E%D7%A7%D7%93%D7%9E%D7%99%D7%9D.pdf
                         'SO2' => 2.62,
                         'NOX' => 1.88,
                         'NO2' => 1.88,
                         'O3' => 1.96,
                         'CO' => 1.15,
                         'BENZN' => 3.19,
                         'TOLUEN' => 3.77,
                         'H2S' => 1.4,
                         '1-3butadiene' => 2.21,
                         # hand-calculated, based on http://www2.dmu.dk/AtmosphericEnvironment/Expost/database/docs/PPM_conversion.pdf
                         # and a temperature of 25C, and molar mass lookup
                         'NO' => (12.187 * 30.0061)/(273.15 + 25),
                         'EthylB' => (12.187 * 106.17)/(273.15 + 25),
                         'O-Xyle' => (12.187 * 106.16)/(273.15 + 25),
);


my %warn_levels_eurostd = (		# European standard for low/medium/high/very high values, in microgram/m^3, from: http://air.net.technion.ac.il/%D7%9E%D7%90%D7%92%D7%A8-%D7%A0%D7%AA%D7%95%D7%A0%D7%99%D7%9D/
		   'NO2' => [50, 100, 200, 400],
		   'O3' => [60, 120, 180, 240],
		   'CO' => [5, 7.5, 10, 20], # this is reported in mg/m^3
		   'SO2' => [50, 100, 350, 500],
		   'PM10' => [25, 50, 90, 180],
		   'PM2.5' => [15, 30, 55, 110],
		  );

my %warn_levels_legal_daily = (		# generally ERECH YAAD from http://www.sviva.gov.il/InfoServices/ReservoirInfo/DocLib/Air/Avir30.pdf
			    'SO2' => [50],
			    'TOLUEN' => [3770],
			    'formaldehyde' => [0.8], # This is in microgram/m^3, while the IGUD report is said to be at miligram/m^3 - I think IGUD is really microgram
			    '1,3 Butadyen' => [0.11],
			    'BENZN' => [3.9],
# Got too noisy, disabled PM10 and PM2.5
#			    'PM10' => [50],
#			    'PM2.5' => [25],
			    'NO2' => [200],
			      );

my %warn_levels_opacity_hourly = (		# hand-crafted
				  'OPCB1' => 20,
				  'OPCB2' => 20,
				  'OPCB3' => 20,
				  'OPB31' => 20,
				  'Opa11' => 20,
				  'Opa21' => 20,
				  'OpaC1' => 20,
				  'OpaC3' => 20,
				  'OpaC4' => 20,
				  'OpaCR' => 20,
				  'OpaFC' => 20,
				  'OpaGH' => 20,
				  'OpaHD' => 20,
				  'OpaV3' => 20,
				  'ISO' => 20,
				  'OP-HCU' => 20,
				  'OP_HPU' => 20,
				  );


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
				'Deshanim:NOX' => [350], # from the following, also saying it should go to 300 on Sep 1 2020 : https://www.gov.il/BlobFolder/dynamiccollectorresultitem/dshanim_emmision_permit_update/he/air_emission_permits_2020_%D7%AA%D7%99%D7%A7%D7%95%D7%9F%20%D7%9E%D7%A1'%202%20-%20%D7%AA%D7%99%D7%A7%D7%95%D7%9F%20%D7%94%D7%99%D7%AA%D7%A8%20%D7%A4%D7%9C%D7%99%D7%98%D7%94%20%D7%93%D7%A9%D7%A0%D7%99%D7%9D_%D7%A1%D7%95%D7%A4%D7%99.pdf
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
                                 'Flares:HHPFlare' => [0.01], # emergency-only  flare, any activity is a sign of malfunction
                                 'SO2' => [350]
			       );

# generally ERECH SVIVA from: http://www.sviva.gov.il/InfoServices/ReservoirInfo/DocLib/Air/Avir30.pdf
my %legal_mean_max_daily = (
			    'SO2' => 50,
			    'TOLUEN' => 3770,
			    'formaldehyde' => 0.8, # This is in microgram/m^3, while the IGUD report is said to be at miligram/m^3 - I think IGUD is really microgram
			    '1,3 Butadyen' => 0.11,
			    'BENZN' => 3.9,
			    'PM10' => 130,
			    'PM2.5' => 37.5,
			    'NO2' => 200,   # for one hour

			    # hand-crafted rules for opacity, based on data from Jan-October 2017, computed as 95% percentile + inter-quartile difference (=third quartile minus first quartile)
			    'OPCB1' => '20:opacity_daily',
			    'OPCB2' => '20:opacity_daily',
			    'OPCB3' => '20:opacity_daily',
			    'OPB31' => '20:opacity_daily',
			    'Opa11' => '20:opacity_daily',
			    'Opa21' => '20:opacity_daily',
			    'OpaC1' => '20:opacity_daily',
			    'OpaC3' => '20:opacity_daily',
			    'OpaC4' => '20:opacity_daily',
			    'OpaCR' => '20:opacity_daily',
			    'OpaFC' => '20:opacity_daily',
			    'OpaGH' => '20:opacity_daily',
			    'OpaHD' => '20:opacity_daily',
			    'OpaV3' => '20:opacity_daily',
			    'ISO' => '20:opacity_daily',
			    'OP-HCU' => '20:opacity_daily',
			    'OP_HPU' => '20:opacity_daily',
			    # 'OPCB1' => '8:opacity_daily',
			    # 'OPCB2' => '9:opacity_daily',
			    # 'OPCB3' => '12:opacity_daily',
			    # 'OPB31' => '6:opacity_daily',
			    # 'Opa11' => '2:opacity_daily',
			    # 'Opa21' => '3.5:opacity_daily',
			    # 'OpaC1' => '10:opacity_daily',
			    # 'OpaC3' => '10:opacity_daily',
			    # 'OpaC4' => '6:opacity_daily',
			    # 'OpaCR' => '3:opacity_daily',
			    # 'OpaFC' => '5:opacity_daily',
			    # 'OpaGH' => '9:opacity_daily',
			    # 'OpaHD' => '5:opacity_daily',
			    # 'OpaV3' => '15:opacity_daily',
			    # 'ISO' => '2.16:opacity_daily',
			    # 'OP-HCU' => '6:opacity_daily',
			    # 'OP_HPU' => '7:opacity_daily',
			    
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
			    'Bazan-CU1-B4:NOx Dry' => 150,	 # stack # 137015
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
			    'GADIV CTO:BENZN' => 2.5, # stack # 176967
			    'Gadiv new:Benzene-Namal' => 1000,
			    'Gadiv-AS2XYL1:NOx Dry' => 150,	 # stack # 8807
			    'Gadiv-Boiler:NOx Dry' => 150,	 # stack # 8815
			    'Gadiv-PAREX:NOx Dry' => 100,	 # stack # 8811
			    'Gadiv-TOL:NOx Dry' => 100,	 # stack # 8809
			    'Gadiv-XYl2:NOx Dry' => 100,	 # stack # 8813
			    'HaifaChem:Nox N1' => 150,
			    'HaifaChem:Nox - N2' => 200,
			    'Deshanim:NOX' => 350, # from the following, also saying it should go to 180 on Sep 1 2020 : https://www.gov.il/BlobFolder/dynamiccollectorresultitem/dshanim_emmision_permit_update/he/air_emission_permits_2020_%D7%AA%D7%99%D7%A7%D7%95%D7%9F%20%D7%9E%D7%A1'%202%20-%20%D7%AA%D7%99%D7%A7%D7%95%D7%9F%20%D7%94%D7%99%D7%AA%D7%A8%20%D7%A4%D7%9C%D7%99%D7%98%D7%94%20%D7%93%D7%A9%D7%A0%D7%99%D7%9D_%D7%A1%D7%95%D7%A4%D7%99.pdf
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
		      
my @skip_sensors_cumulative = (         #                   Encode as station:sensor
                              # new pattern as of late April 2016, correlated with renovation works at the complex; should take them out of this list in a few months.
#                              'OP-BZN1:(B201B CU3|STAT2|cu1 B4|OPB31|Opa[CV]3)',
                              # new pattern, as of around 12/7/2016
                                        # inhibited 6/1/2017 'OP-BZN1:STAT5',
                              # new pattern, as of around 15/8/2016
                                        # inhibited as of 6/1/2017 'OP-BZN1:STAT3',
                              # another new pattern, as of around 15/8/2016
                                        # inhibited as of 6/1/2017 'OP-GADIV:XLNHT',
                                        # inhibited as of 6/3/2017 CAOL_New:Boiler[123]-Stat',
                                        # inhibited as of 6/3/2017 'BAZAN_Oper:(HVGO HSD|Benzine HDS|CU\.4)',
                                        # new pattern, as of Jul 2017
                                        # inhibited as of 6/1/2017 'CAUL:OPCB1',
                                        # new pattern, as of around late December 2016
                                        # inhibited as of 6/3/2017 'GADIV_OPER:(XYL|PRX)-start',
                                        'Mobile.*:.*',
    );

my $skip_sensors_cumulative = '^(' . join("|", @skip_sensors_cumulative) . ')$';

my @skip_sensors_hourly = (         #                   Encode as station:sensor
                                    'BAZAN-B11:Stack_NOx', # started in late March, still persisting as of May 22
                                    'BAZAN-B21:NOx Dry',  # started in late March, still persisting as of May 22
                                    'Mobile_New:',
                                    'Mobile 5:',
                                    'Mobile 6:',
    );

my $skip_sensors_hourly = '^(' . join("|", @skip_sensors_hourly) . ')$';

my @skip_sensors_legal_limits = (         #                   Encode as station:sensor
    );

my $skip_sensors_legal_limits = '^(' . join("|", @skip_sensors_legal_limits) . ')$';

# sensors which only include pollutants
my @nonscada_sensors = qw/
			   SO2
			   NOX
			   TOLUEN
			   BENZN
			   PM10
			   PM2.5
			   NO2
			   NO
			   O3
			   formaldehyde
			   O-Xyle
			   EthylB
			   NOXEMISS
			   CO
			   1-3butadiene
		       /;

my $nonscada_sensors = '^(' . join("|", @nonscada_sensors) . ')$';

# sensors which are known to not produce data
my @skip_sensors_missing = (         #                   Encode as station:sensor
			    'Delek:TOC',
			    'Elcon:.*', # since 1-Aug-2016
			    'Kiryat Binyamin:(1-3butadiene|PM10)',
			    'bazan NOx emission rate:.*',
			    '(Romema|Carmelia|Yizraelia):((LAF|LXpk)_(max|min)|L[XA]eq|LAim)',
			    'Kiryat Haim-Regavim:M\+P-XY',
			    'Kiryat Haim-Degania:SO2', # since around 20-Jun-2016
                'Kiryat Haim-Degania:spare', # around 15-July-2017
                'Kiryat Haim-Degania:.*', # Name change
			    'OP-GADIV:STBLR',
                                     # inhibited as of 6/3/2017 'Gadiv Nox emission rate:.*',
			    'Shemen:TOC', # since 1-Sep-2016
                                     'Tashan fuel port:TOC',  # maybe 1 reading once a month
                                     'Neve Yosef:CO',         # stopped at 10/11/2016
                                     'Neve Yosef:PM10',         # stopped at 27/3/2017
                                     'Neve Yosef:NOX',         # stopped on April 2020
                                     'Neve Yosef:NO(|2)',         # stopped late October 2020
                                     #'Igud \(check-post\):SO2', #stopped at 14/11/2016, back sometime before 3/2017
                                     'Igud \(check-post\):O-Xyle', #stopped at 5/1/2017
                                     'OP-BZN1:ISO', # stopped at 5/1/2017, back sometime before 4/2017
                                     'Einstein:.*', # maintenance starting Mar 2017, going to be managed by the electric company
                                     'Ahuza:.*', # maintenance during Mar 2017
                                     'SHOOK:.*', # new station up in March 2017, not yet transmitting data
                                     'Mobile.*:.*',
                                     'Kiryat Yam:SO2', # stopped at 20/2/2017
                                     #  'Kfar Hasidim:SO2', # stopped sometime before 3/2017, back in 4/2017
                                     'Gadiv Nox emission rate:Boiler-Nox emission rate', # stopped around 20 April 2017
                                     'Gadiv-Boiler:NOx Dry', # stopped at 23 April 2017
                                     'Gadiv-Boiler:NOX INS', # stopped in May 2017
                                     'Nesher:(PM2\.5|pm\(10-2\.5\)|PM10)', #stopped in early August 2017,
                                     'OP-GADIV:STAT2', #stopped sometime in June 2017
                                     'Shprintzak:SO2', #stopped 2 Dec 2017
                                     'Kiryat Bialik Ofarim:SO2',
                                     'Kiryat Haim-Regavim:spare',
                                     'Nesher:CO', # stopped 20 Dec 2017
                                     'Kiryat Ata:PM2\.5', #stopped 1 Nov 2017
                                     'Kiryat Ata:pm\(10-2\.5\)', #stopped 1 Nov 2017
                                     'Kiryat Motzkin Acco road:.*', #since 14-Mar-2018, for ramp-up
                                     'Atzmaut:.*', #since 14-Mar-2018, for ramp-up
                                     'Hugim:.*', #since 14-Mar-2018, for ramp-up
                                     'HaifaChem:.*', # as of 30/4/2019
                                     'OP-BZN1:(Opa21|Opa11|OpaFC|OPB31)', # OpaFC down before 1/1/2019; the others shut off 30/4/2019
                                     'Kiryat Bialik:.*',            # No data after June 2017. Name change?
                                     'Sonol:TOC',                   # 1/3/2019
                                     'Kiryat Motzkin:SO2',          # unclear (rename?)
                                     'BAZAN TO-1 2:.*',		   # 1/1/2020
                                     'BAZAN TO-4:.*',		   # 1/1/2020
                                     'BAZAN VRU:.*',		   # 1/1/2020
                                     'Bazan-CU1:.*', # 1/1/2020
                                     'Bazan-CU1-B4:.*', # 1/1/2020
                                     'BAZAN-CU4:.*', # 1/1/2020
                                     'BAZAN6-SO2:.*', # 1/1/2020
                                     'BAZAN-B11:.*', # 1/1/2020
                                     'BAZAN-CCR:.*', # 1/1/2020
                                     'BAZAN HCU:.*', # 1/1/2020
                                     'BAZAN-B21:.*', # 1/1/2020
                                     'BAZAN-B31:.*', # 1/1/2020
                                     'BAZAN-FCC:.*', # 1/1/2020
                                     'BAZAN-IHDS-GO:.*', # 1/1/2020
                                     'BAZAN-ISOM:.*', # 1/1/2020
                                     'BAZAN_HDS:.*', # 1/1/2020
                                     'BAZAN_OPER:.*', # 1/1/2020
                                     'BAZAN_Oper:.*', # 1/1/2020
                                     'Bazan-CU3:.*', # 1/1/2020
                                     'Bazan-CU3-B201B:.*', # 1/1/2020
                                     'Bazan-VIS3:.*', # 1/1/2020
                                     'Kiryat Haim-Regavim:O3', # 1/1/2020
                                     'OP-BZN1:.*', # 1/1/2020
                                     'OP-HCU:.*', # 1/1/2020
                                     'BAZAN_HPU:.*', # 8/2/2020
                                     'BAZAN.VRU:.*', # 1/1/2020
                                     'CAOL.(RTO|B1|B2|B3):.*', # 15/2/2020
                                     'GADIV (RCO|PAREX):.*', # 15/2/2020
                                     'GADIV_OPER:.*', # 17/2/2020
                                     'GADIV-(XYl2|TOL|AS2XYL1|PAREX):.*', # 17/2/2020
                                     'TOREN:.*', # 18/2/2020
                                     'Gadiv-(XYl2|TOL|AS2XYL1|PAREX):.*', # 19/2/2020
                                     'GADIV.(TOL|AROM|PARX|XYL):.*', # 18/2/2020
                                     'BAZAN.(CCR|HCU|HPU|FCC|HDSGO|VRU|VIS3|SRU3|CU1|CU3|CU4|B201B|B11|B21|B31|SRU4|ISO|TO1|TO4|C120\(HDS\)):.*', # 15/2/2020
                                     'HDS Hvgo down:.*', # 19/2/2020
                                     'CAOL_New:.*', # 19/2/2020
                                     'Masofei Nipuk:.*', # 20/2/2020
                                     'Haifa Power Unit [34]:.*', # 1/6/2020
                                     'mahzam [34]0:.*', # 10/5/2020
                                     'Romema:PM10', # 10/2/2020
                                     'Romema:CO', # 1/9/2020
                                     'Yizraelia:.*', # 20/10/2020
                                     'TARO:.*', # 16/10/2020
                                     'Carmelia:CO', # Before 11/2019
                                     'Carmelia:.*', # 9/11/2020
			    'Kiryat Haim-Regavim:.*', # Moved to "Kiryat Haim-West" 21 July 2020
    );

my $skip_sensors_missing = '^(' . join("|", @skip_sensors_missing) . ')$';

# sensors which look like status sensors, but the values seem more like numeric
my @skip_sensors_state = (         #                   Encode as station:sensor
                                   'OP-BZN1:STAT[1-5]',
                                   #inhibited as of 6/3/2017 'OP-GADIV:STAT[12]',
                                   'OP-BZN1:OPB31-Stat',
    );

my $skip_sensors_state = '^(' . join("|", @skip_sensors_state) . ')$';

getopts("n:", \%options);

my $NOW = $options{'n'};
my $DEBUG = defined($NOW);

if(!defined($NOW)) {
  $NOW = time();
} elsif($NOW =~ m!(\d+)/(\d+)/(\d+) (\d+):(\d+)!) {
  my ($mday, $mon, $year, $hour, $min) = ($1, $2, $3, $4, $5);
  $NOW = timelocal(0, $min, $hour, $mday, $mon-1, $year);
} else {
  die "Unknown time format: $NOW";
}

my $hist_root = 'envdata_hist';
my $data_root = 'envdata';
my $stats_file = 'db/weekstats';

my $climate_sensors = '^(RH|BP|WDS|TEMP|WDD|SR|PREC|StWd|WIND-V6|ITemp|FILTER.*|Filter|WIND-DIR|RAIN)$';

my $users = load_userdb();
if (!defined($users) || (scalar(keys %$users) == 0)) {
  $users = { 'dp' => {'Name' => 'Dan P', 'phone' => '052-5536156', 'email' => 'dan-envalert@pelleg.org', 'paniclevel' => 3}};
  store_userdb($users);
}

# get historical data and latest readings
my $hist_stats = get_historical_stats();

my $latest_data = get_current_data(2);

# generate alerts
my @msgs;
for my $station (keys %$latest_data) {
  my @sensors = keys %{$latest_data->{$station}};
  for my $sensor (@sensors) {
    unless ($sensor =~ /$climate_sensors/) {
      my $msgs = check_station_and_sensor($latest_data, $hist_stats, $station, $sensor);
      push(@msgs, @$msgs);
    }
  }
}

# send recent data for external processing of AQI
{
  my $msgs = compute_AQI($latest_data);
  if($msgs) {
    push(@msgs, @$msgs);
  }
}

#push alerts into message queue
my $mq = load_msgdb();

for my $msg (@msgs) {
  my $msg_rule = $msg->{'rule'} || "";
  my $txt = "";
  if($msg_rule =~ /^zscore247/) {
    my $mean = $msg->{'mean'};
    my $std = $msg->{'std'};
    $txt = sprintf("%s: sensor %s at %s: reading of %s %s, compared to typical value of %s%s (severity %d)",
		      strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		      $msg->{'sensor'},
		      $msg->{'station'},
		      my_format($msg->{'reading'}),
		      $msg->{'units'},
		      my_format($mean),
		      ($std < $mean) ? sprintf(" +/- %s", my_format($std)) : "",
		      $msg->{'severity'}
		     );
  } elsif($msg_rule eq 'eurostd') {
    $txt = sprintf("%s: sensor %s at %s: reading of %s %s, which is defined '%s' by the European standard (severity %d)",
		      strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		      $msg->{'sensor'},
		      $msg->{'station'},
		      my_format($msg->{'reading'}),
		      $msg->{'units'},
		      $msg->{'warnlevel'},
		      $msg->{'severity'}
		      );
  } elsif($msg_rule eq 'curr_hourly_max') {
    $txt = sprintf("%s: sensor %s at %s: reading of %s %s, which is above the HEITER PLITA for the half-hour measurement (%s)",
		      strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		      $msg->{'sensor'},
		      $msg->{'station'},
		      my_format($msg->{'reading'}),
		      $msg->{'units'},
		      $msg->{'warn_threshold'}
		      );
  } elsif($msg_rule eq 'curr_daily_max') {
    $txt = sprintf("%s: sensor %s at %s: reading of %s %s, which is above the standard for the daily mean (%s)",
		      strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		      $msg->{'sensor'},
		      $msg->{'station'},
		      my_format($msg->{'reading'}),
		      $msg->{'units'},
		      $msg->{'warn_threshold'}
		      );
  } elsif($msg_rule =~ /^zscore_cumulative/) {
    my $mean = $msg->{'mean'};
    my $std = $msg->{'std'};
    $txt = sprintf("%s: sensor %s at %s: average daily value of %s %s, compared to typical value of %s%s (severity %d)",
		      strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		      $msg->{'sensor'},
		      $msg->{'station'},
		      my_format($msg->{'reading'}),
		      $msg->{'units'},
		      my_format($mean),
		      ($std < $mean) ? sprintf(" +/- %s", my_format($std)) : "",
		      $msg->{'severity'}
		     );
  } elsif($msg_rule =~ /^legal_max_daily/) {
    $txt = sprintf("%s: sensor %s at %s: average daily value of %s %s, above clean-air regulation maximum of %s",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		   $msg->{'sensor'},
		   $msg->{'station'},
		   my_format($msg->{'reading'}),
		   $msg->{'units'},
		   my_format($msg->{'max'}),
		  );
  } elsif($msg_rule =~ /^opacity_daily/) {
    $txt = sprintf("%s: sensor %s at %s: average daily value of %s %s, above reasonable value of %s",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		   $msg->{'sensor'},
		   $msg->{'station'},
		   my_format($msg->{'reading'}),
		   $msg->{'units'},
		   my_format($msg->{'max'}),
		  );
  } elsif($msg_rule =~ /^opacity_hourly/) {
    $txt = sprintf("%s: sensor %s at %s: value of %s %s, above reasonable value of %s",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		   $msg->{'sensor'},
		   $msg->{'station'},
		   my_format($msg->{'reading'}),
		   $msg->{'units'},
		   my_format($msg->{'warn_threshold'}),
		  );
  } elsif($msg_rule =~ /missing.*data/) {
    $txt = sprintf("%s: sensor %s at %s: %d missing data points (%d consecutive)",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		   $msg->{'sensor'},
		   $msg->{'station'},
		   $msg->{'num_missing'},
		   $msg->{'gap'},
		  );
  } elsif($msg_rule eq 'state_change') {
    $txt = sprintf("%s: sensor %s at %s: changed state from %s to %s",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
		   $msg->{'sensor'},
		   $msg->{'station'},
		   $msg->{'prev'},
		   $msg->{'curr'},
		  );
  } elsif($msg_rule eq 'AQI') {
    my $aqi = $msg->{'AQI'};
    $txt = sprintf("%s: air-quality index %s at %s: dominant pollutant is %s",
		   strftime("%d/%m/%y %H:%M", localtime($msg->{'read_time'})),
                   $aqi < 0 ? "($aqi)" : $aqi,
		   $msg->{'station'},
		   $msg->{'sensor'},
		  );
  } else {
    warn "Unknown message-generation rule $msg_rule";
  }
  if($DEBUG) {
    print STDERR "$msg_rule: $txt\n";
  }
  for my $user (keys %$users) {	# we could optimize here by considering the user's threshold value
    my %msg_record = ('txt' => $txt,  'recipient' => $user);
    for my $k (qw/read_time detection_time station sensor severity warnlevel rule/) {
      if(exists($msg->{$k})) {
	$msg_record{$k} = $msg->{$k};
      }
    }
    push(@$mq, \%msg_record);
  }
}

store_msgdb($mq);

#smoothed reading value: last two of $latest_data->{$station}->{$sensor}->{'reads'}
#  get historical mean and std from $stats->{$station}->{$sensor}->{$dow}->{$tod}
#  alert is smoothed value is more than historical mean + 2 stds


sub get_current_data {
  my ($days_back) = @_;
  $days_back = 1 unless defined($days_back);
  my $ret = {};

  my $when = DateTime->from_epoch(epoch => $NOW);
  for(0..$days_back) {
    my $datadir = sprintf("%s/%s", $data_root, $when->ymd('/'));
    if(-d $datadir) {
      my $data = slurp_subdir_multistation($datadir);
      $ret = merge_two_tables($ret, $data);
    }
    $when->subtract(days => 1);		# go back one day
  }
  return $ret;
}

sub get_historical_stats {
  my $ret;
  if(-e $stats_file) {
    $ret = lock_retrieve($stats_file);
  } else {
    # compute all 24x7 averages, if missing
    my $data;
    # step over historical months until horizon
    my $month = DateTime->from_epoch(epoch => $NOW);    $month->truncate(to => 'month');
    my $NOW_DT = DateTime->from_epoch(epoch => $NOW);   $NOW_DT->truncate(to => 'month');

    while(($NOW_DT->epoch - $month->epoch) < $history_lookback) {
      my $dirname = sprintf("%s/%4d/%02d", $hist_root, $month->year, $month->month);
      if (-d $dirname) {
        my $more_data = slurp_subdir_multistation($dirname);
 	    $data = merge_two_tables($data, $more_data);
      }
      $month->subtract(months => 1);
    }

    # DEBUG my $data = slurp_sensor_table_csv('envdata_hist/envhaifa-201501.csv');
    $ret = compute_weekly_stats($data);
    # save for next time
    lock_store($ret, $stats_file);
  }
  return $ret;
}

sub compute_weekly_stats {
  my ($data) = @_;
  my $ret = {};

  # tabulate and aggregate
  for my $station (keys %$data) {
    my $h = $data->{$station};
    for my $sensor (keys %$h) {
      my $hh = $h->{$sensor};
      if(!exists($ret->{$station}->{$sensor})) {
        $ret->{$station}->{$sensor} = {};
      }
      my $cell = $ret->{$station}->{$sensor};
      for my $ts (keys %{$hh->{'reads'}}) {
        my $val = $hh->{'reads'}->{$ts};
        if(($ts > ($NOW - $history_lookback)) && defined($val)) {
          my ($dow, $tod) = ts_to_dow_tod($ts);
          if(!exists($cell->{$dow}->{$tod})) {
            $cell->{$dow}->{$tod} = {};
          }
          my $microcell = $cell->{$dow}->{$tod};
          $microcell->{'n'}++;
          $microcell->{'sum'} += $val;
          $microcell->{'sumsqd'} += $val*$val;
        }
      }
    }
  }

  # compute means and stds
  for my $station (keys %$ret) {
    my $h = $ret->{$station};
    for my $sensor (keys %$h) {
      my $hh = $h->{$sensor};
      for my $dow (keys %$hh) {
        my $hhh = $hh->{$dow};
        for my $tod (keys %$hhh) {
          my $hhhh = $hhh->{$tod};
          my $N = $hhhh->{'n'};
	  my $sum = $hhhh->{'sum'};
          my $mean = $sum/$N;
          my $sumsqd = $hhhh->{'sumsqd'};
          my $stdsqd = stdsqd_from_moments($N, $sum, $sumsqd);
          $hhhh->{'mean'} = $mean;
          $hhhh->{'std'} = sqrt($stdsqd);
        }
      }
    }
  }
  return $ret;
}

sub check_station_and_sensor {
  my ($latest_data, $hist_stats, $station, $sensor) = @_;
  my $complained_missing_data = 0;
  my @ret = ();
  push(@ret, @{check_station_and_sensor_hourly_reading($latest_data, $hist_stats, $station, $sensor)});
  push(@ret, @{check_station_and_sensor_cumulative($latest_data, $hist_stats, $station, $sensor)});
  push(@ret, @{check_station_and_sensor_legal_limits($latest_data, $hist_stats, $station, $sensor)});
  push(@ret, @{check_station_and_sensor_legal_limits($latest_data, $hist_stats, $station, $sensor, 23)});
  push(@ret, @{check_station_and_sensor_missing($latest_data, $hist_stats, $station, $sensor)});
  # need to figure out how to reduce false positives for state change
  #push(@ret, @{check_station_and_sensor_state_change($latest_data, $hist_stats, $station, $sensor)});
  # For debugging, also check deviations downward
  # push(@ret, @{check_station_and_sensor_hourly_reading($latest_data, $hist_stats, $station, $sensor, -1)});
  return \@ret;
}

# check if today's readings, accumulated since midnight, is above the cumulative value of the historic values
sub check_station_and_sensor_cumulative {
  my ($latest_data, $hist_stats, $station, $sensor) = @_;
  my @ret = ();

  my $lookup_skip_key = "$station:$sensor";
  if($lookup_skip_key =~ /$skip_sensors_cumulative/) {
    return \@ret;
  }
  my $latest_cell = $latest_data->{$station}->{$sensor}->{'reads'};
  my $units =       $latest_data->{$station}->{$sensor}->{'units'};

  if (!defined($latest_cell)) {
    return \@ret;
  }

  # find out the time now
  my $dt_now = DateTime->from_epoch(epoch => $NOW, time_zone => $envlib::TZ_HERE);
  # wait until enough observations accumulate
  if($dt_now->hour() >= 5) {
    
    # array of today's readings, indexed by the time in whole hours (0=midnight)
    my @val_today = @{get_daily_readings($latest_cell)};
    my $latest_reading = $#val_today;

    my $sum_today = 0;
    map {
      $sum_today += $_ if(defined($_));
    } @val_today;
    
    # now get the historic values
    my ($now_dow, $now_tod) = ts_to_dow_tod($NOW);
    my ($hist_n, $hist_sum, $hist_sumsqd) = (0, 0, 0);
    for my $tod (0..$latest_reading) {
      $tod = sprintf("%02d", $tod);
      my $hist_data = $hist_stats->{$station}->{$sensor}->{$now_dow}->{$tod};
      if(defined($hist_data)) {
	$hist_n += $hist_data->{'n'};
	$hist_sum += $hist_data->{'sum'};
	$hist_sumsqd += $hist_data->{'sumsqd'};
      }
    }

    if($hist_n > 0) {
      my $hist_mean = $hist_sum / $hist_n;
      my $hist_std = sqrt(stdsqd_from_moments($hist_n, $hist_sum, $hist_sumsqd));
    
      my $zscore = ($sum_today - $hist_mean)/max($hist_std, 1e-3);

      # if std is very low (or zero), skip this test
      my $severity = $hist_std > 1e-3 ? floor($zscore) : 0;

      my $rule = 'zscore_cumulative';
      if($sensor =~ /$nonscada_sensors/) {
	$rule .= '_nonscada';
      }

      if ($severity >= 2) {
	# round detection time to hour
	my $detection_time = int($NOW/3600)*3600;
	push(@ret, {
		    'station' => $station, 'sensor' => $sensor, 'reading' => $sum_today/(1+$latest_reading), 'read_time' => $detection_time,
		    'detection_time' => $detection_time, 'units' => $units, 'severity' => $severity, 'mean' => $hist_mean/(1+$latest_reading),
		    'std' => $hist_std/(1+$latest_reading),
		    'rule' => $rule});
      }
    }
  }

  return \@ret;
}

# check if a status indicator change its state
sub check_station_and_sensor_state_change {
  my ($latest_data, $hist_stats, $station, $sensor) = @_;
  my @ret = ();


  if(!($sensor =~ m/stat/i || $sensor =~ m/^((SPL|PRX|XLN|TLN)HT|CU\.[134]|Vis3\.|FCC|HVGO HSD|KERO HDS|Benzine HDS|CCR|GO HDS|Isomer|B11|B21|Boiler31)$/)) {
    return \@ret;
  }

  my $lookup_skip_key = "$station:$sensor";
  if($lookup_skip_key =~ /$skip_sensors_state/) {
    return \@ret;
  }

  my $latest_cell = $latest_data->{$station}->{$sensor}->{'reads'};
  my $units =       $latest_data->{$station}->{$sensor}->{'units'};

  if (!defined($latest_cell)) {
    return \@ret;
  }
  
  my @all_ts = sort { $a <=> $b } keys %$latest_cell;

  @all_ts = grep { $_ <= $NOW } @all_ts; # ignore stuff in the future if -n is used
  
  my $curr = pop(@all_ts);
  my $prev;
  # find most recent defined value
  do {
    $prev = pop(@all_ts);
  } while(!defined($latest_cell->{$prev}));

  my $state_prev = $latest_cell->{$prev};
  my $state_curr = $latest_cell->{$curr};

  if($state_prev ne $state_curr) {
    my $str_prev = state_to_str($state_prev);
    my $str_curr = state_to_str($state_curr);
    push(@ret, {
		'station' => $station, 'sensor' => $sensor,
		'read_time' => $curr,
		'curr' => $str_curr,
		'prev' => $str_prev,
		'units' => $units, 'severity' => 200,
		'rule' => 'state_change'
		});
  }

  return \@ret;
}

sub state_to_str {
  my ($s) = @_;
  my %m = ('1' => 'OK', '2' => 'problem', '3' => 'grounded', '4' => 'calibration', '5' => 'meter fault', '6' => 'NISHUF', '7' => 'maintenance', '8' => 'startup', '9' => 'shutdown');
  my $s_mnemonic = $m{$s};
  if(defined($s_mnemonic)) {
    $s .= " ($s_mnemonic)";
  }
  return $s;
}

# helper function to formulate the readings in a given day as an array indexed by the hour
sub get_daily_readings {
  my ($latest_cell) = @_;

  # array of today's readings, indexed by the time in whole hours (0=midnight)
  my @ret;      

  # find out the time now
  my $dt_now = DateTime->from_epoch(epoch => $NOW, time_zone => $envlib::TZ_HERE);
  for my $ts (keys %$latest_cell) {
    # remove datapoints in the future (needed when we supply a fake reference date in the command line
    next if($ts > $NOW);
    # only look at today's readings
    my $dt_reading = DateTime->from_epoch(epoch => $ts, time_zone => $envlib::TZ_HERE);
    next unless($dt_reading->ymd() eq $dt_now->ymd());
      
    my $v = $latest_cell->{$ts};
    my ($dow, $tod) = ts_to_dow_tod($ts);
    $ret[$tod] = $v;
  }
  return \@ret;
}

# check if yesterday's average readings exceed the legally allowed daily values
sub check_station_and_sensor_legal_limits {
  my ($latest_data, $hist_stats, $station, $sensor, $not_before_hour) = @_;
  my @ret = ();
  my $severity = 230;
  my $fudge_factor = 1;

  my $rule = 'legal_max_daily';
  if(defined($not_before_hour)) {
    $rule .= "_$not_before_hour";
    $severity += 50;
  } else {
    $not_before_hour = 10;
  }
  my $lookup_skip_key = "$station:$sensor";
  if($lookup_skip_key =~ /$skip_sensors_legal_limits/) {
    return \@ret;
  }

  # first try the specific value for this particular sensor
  my $key = "$station:$sensor";
  my $legal_max;
  $legal_max = $legal_mean_max_daily{$key};
  if(defined($legal_max)) {
    $fudge_factor = 1.2; # fudge factor until I figure out the HEITER PLITA calculation
  } else { # if no specific value, try the per-pollutant value
    $legal_max = $legal_mean_max_daily{$sensor};
  }
  if(!defined($legal_max)) {
    return \@ret;
  }
  # check if the threshold value encodes a rule
  if($legal_max =~ /^(.*):(.*)$/) {
    $legal_max = $1;
    $rule = $2;
  }
  
  my $latest_cell = $latest_data->{$station}->{$sensor}->{'reads'};
  my $units =       $latest_data->{$station}->{$sensor}->{'units'};

  if (!defined($latest_cell)) {
    return \@ret;
  }

  # find out the time now
  my $dt_now = DateTime->from_epoch(epoch => $NOW, time_zone => $envlib::TZ_HERE);
  # wait until enough observations accumulate
  if($dt_now->hour() >= $not_before_hour) {
    my @val_today = @{get_daily_readings($latest_cell)};
    my $latest_reading = $#val_today;

    my $sum_today = 0;
    my $num_today = 0;

    map {
      my $h = $_;
      my $v = $val_today[$h];
      if(defined($v) && ($v >= 0)) {
        $sum_today += $v;
        $num_today++;
      }
    } 0..$latest_reading;
    
    if($num_today > 0) {
      my $mean_today = $sum_today/$num_today;

      ($mean_today, $units) = convert_units($station, $sensor, $mean_today, $units);

      if($mean_today > ($fudge_factor * $legal_max)) {
	# round detection time to hour
	my $detection_time = int($NOW/3600)*3600;
	push(@ret, {
		    'station' => $station, 'sensor' => $sensor, 'reading' => $mean_today, 'read_time' => $detection_time,
		    'detection_time' => $detection_time, 'units' => $units, 'severity' => $severity, 'max' => $legal_max, 
		    'rule' => $rule});
      }
    }
  }
  
  return \@ret;
}


# check if there are too many missing values
sub check_station_and_sensor_missing {
  my ($latest_data, $hist_stats, $station, $sensor) = @_;
  my @ret = ();

  my $lookup_skip_key = "$station:$sensor";
  if($lookup_skip_key =~ /$skip_sensors_missing/) {
    return \@ret;
  }
  
  my $latest_cell = $latest_data->{$station}->{$sensor}->{'reads'};
  my $units =       $latest_data->{$station}->{$sensor}->{'units'};

  # find out the time now
  my $dt_now = DateTime->from_epoch(epoch => $NOW, time_zone => $envlib::TZ_HERE);
  # wait until enough observations accumulate
  my $now_hour = $dt_now->hour();
  if($now_hour >= 10) {
    my @val_today = @{get_daily_readings($latest_cell)};

    # stick a sentinel to simplify the case where the missing span covers the end, and the case of all-values missing
    $val_today[$now_hour+1] = 1;
    
    # print STDERR join(":", $station, $sensor, $now_hour, @val_today) . "\n";
    
    my $num_missing = 0;
    my $longest_gap = 0;
    my $gap_start = undef;

    map {
      my $h = $_;
      if(!defined($val_today[$h])) { # if we're at the sentinel value beyond the end, pretend it's an undef
        $num_missing++;
        $gap_start = $h unless defined($gap_start);
      } else {			# was there a gap we didn't register?
        if(defined($gap_start)) {
          my $gap_length = $h - $gap_start;
          if($gap_length > $longest_gap) {
            $longest_gap = $gap_length;
          }
          $gap_start = undef;
        }
      }
    } 0..(1+$now_hour);	# count one more, last one is a sentinel

    my $ratio_missing = 1.0*$num_missing / (1+$now_hour);

    my $rule;
    
    if($num_missing > 4 && (($ratio_missing > 1/4) || ($longest_gap >= 6))) {
      $rule = 'missing_data';
    }
    if($num_missing >= 18) {
      $rule = 'missing_lots_data';
    }
    
    if($rule) {
      my $detection_time = int($NOW/3600)*3600;
      push(@ret, {
		  'station' => $station, 'sensor' => $sensor, 'num_missing' => $num_missing, 'read_time' => $detection_time,
		  'detection_time' => $detection_time, 'units' => $units, 'severity' => 100+int(50*$ratio_missing), 'ratio_missing' => $ratio_missing,
		  'gap' => $longest_gap,
		  'rule' => $rule});
    }
  }
  
  return \@ret;
}

# check if the hourly reading is above the historic values for this time-of-day/day-of-week
sub check_station_and_sensor_hourly_reading {
  my ($latest_data, $hist_stats, $station, $sensor, $sign) = @_;
  $sign = +1 unless defined($sign);
  my $complained_missing_data = 0;
  my ($dow_NOW, $tod_NOW) = ts_to_dow_tod($NOW);
  
  my @ret = ();

  my $latest_cell = $latest_data->{$station}->{$sensor}->{'reads'};
  my $units =       $latest_data->{$station}->{$sensor}->{'units'};

  my $lookup_skip_key = "$station:$sensor";

  if (!defined($latest_cell)) {
    return \@ret;
  }

  if($lookup_skip_key =~ /$skip_sensors_hourly/) {
    return \@ret;
  }

  my @all_ts = keys %$latest_cell;
  my @sorted_ts = sort { $b <=> $a } @all_ts;
  # remove datapoints in the future (needed when we supply a fake reference date in the command line
  @sorted_ts = grep {$_ <= $NOW} @sorted_ts;

  # only use the last readings
  @sorted_ts = splice(@sorted_ts, 0, 10);

  for my $ts (@sorted_ts) {
    my $v = $latest_cell->{$ts};
    my ($dow, $tod) = ts_to_dow_tod($ts);

    my $hist_data = $hist_stats->{$station}->{$sensor}->{$dow}->{$tod};
    my $mean = $hist_data->{'mean'};
    my $std = $hist_data->{'std'};
    my $n = $hist_data->{'n'};
    my $inhibit = 0;        # some stations are known data black holes, ignore those
    # Note: Igud sensors Ethyl-B, TULOEN, BENZN, O-XYLE, had been off the grid on Wednesdays 9-13 for a rather long time. Only started becoming active then on 13 July 2016
#    $inhibit = 1 if($station eq 'D.CARMEL'); # new station
#    $inhibit = 1 if($tod <= 10);	     # they only started reporting the midnight-to-10am data recently
#    $inhibit = 1 if($tod >= 15 && $tod <= 18 && $sensor eq 'SO2'); # this band is suspiciously under-reported
#    $inhibit = 1 if($tod >= 22 && $tod <= 24 && $sensor eq 'CO'); # this band is suspiciously under-reported
#    $inhibit = 1 if($station eq 'Ahuza' && $sensor eq 'CO');
#    $inhibit = 1 if($station =~ /^(Nesher|Ahuza|Einstein)/ && $sensor eq 'SO2');
#    $inhibit = 1 if($station eq 'Neve Shaanan' && $sensor =~ /^(NO[2X]?|O3|SO2)/);
    $inhibit = 1 if($station eq 'Kiryat Haim-Degania' && $sensor eq 'SO2'); #  unclear issue which started mid-May 2017
#    $inhibit = 1 if($station eq 'Ahuza' && $sensor =~ /^NO/);
    $inhibit = 1 if($station eq 'Ahuza' && $sensor =~ /^PM10/); # was down for a while, back up in late march 2017
#    $inhibit = 1 if($station eq 'BAZAN-FCC' && $sensor eq 'NOx Dry'); #  new sensor (early 2016)
#    $inhibit = 1 if($station eq 'GADIV CTO' && $sensor eq 'BENZN');   #  new sensor (early 2016)
#    $inhibit = 1 if($station eq 'Carmelia' && $sensor eq 'CO');
#    $inhibit = 1 if($station eq 'Dor chemicals');   #      # New station, brought online June 19 2016
    $inhibit = 1 if($station eq 'Gadiv Nox emission rate');   # Started getting an error on Oct 27 2016, not sure why
    $inhibit = 1 if($station eq 'bazan NOx emission rate');   # Started getting an error on Oct 27 2016, not sure why
#    $inhibit = 1 if($sensor eq 'TOC');
#    $inhibit = 1 if($station =~ /^(mahzam [34]0)/); # Started getting error in late Feb 2017
#    $inhibit = 1 if($station eq 'Kfar Hasidim'); # Started getting error in late Feb 2017
    # $inhibit = 1 if($station eq 'Kiryat Yam' && $sensor =~ /^NO/); # sensing started Mar 2017
    # $inhibit = 1 if($station eq 'Kfar Hasidim' && $sensor =~ /^(NO|O3)/); # sensing started Mar 2017
    # $inhibit = 1 if($station eq 'OP-GADIV' && $sensor eq 'STBLR'); # new sensor fron June 2017? unclear
    $inhibit = 1 if($station eq 'Dor chemicals' && $sensor eq 'TOC'); # new sensor fron June 2017
    $inhibit = 1 if($station eq 'TARO' && $sensor eq 'NTOC'); # new sensor fron October 2017
    $inhibit = 1 if($station eq 'Kiryat Bialik Ofarim'); # new sensor fron June 2017
    $inhibit = 1 if($station eq 'Kiryat Motzkin Begin'); # new sensor fron July 2017
    $inhibit = 1 if($station eq 'Kiryat Haim-Degania' && $sensor eq 'PM2.5'); # problems starting Oct 2017
    $inhibit = 1 if($station eq 'Kiryat Haim-Degania'); # name change
    $inhibit = 1 if($station eq 'Hadar'); #new sensor from November 2017
    $inhibit = 1 if($station eq 'Ahuza transportation'); #new sensor from November 2017
    $inhibit = 1 if($station eq 'Kiryat Ata' && $sensor =~ 'PM(2\.5|10)'); # disabled Nov 2017
    $inhibit = 1 if($station eq 'Kiryat Haim-Regavim' && $sensor eq 'spare'); # problems starting Oct 2017
    $inhibit = 1 if($station eq 'Nesher' && $sensor eq 'CO'); # disabled since mid-November 2017
    $inhibit = 1 if($station eq 'Kiryat Bialik Ofarim' && $sensor eq 'SO2'); # disabled since mid-November 2017

    if (!defined($mean) || !defined($std) || !defined($n)) {
      if (!$complained_missing_data) {
        if(!$inhibit
           && ($tod_NOW == $tod)	# only emit a warning once (otherwise, future invokations will look back at this data and repeat the warning)
            ) {
          # warn "No historical data for station $station, sensor $sensor day $dow hour $tod\n";   # I'm getting too many of those
        }
      }
      $complained_missing_data = 1;
      next;
    }

    if ($n < 15) {
      if (!$inhibit &&
	  !$complained_missing_data
	  && ($tod_NOW == $tod)	# only emit a warning once (otherwise, future invokations will look back at this data and repeat the warning)
	 ) {
        # warn "Not enough historical observations for station $station, sensor $sensor day $dow hour $tod\n";
      }
      $complained_missing_data = 1;
    }

    my $zscore = $sign*(($v - $mean)/max($std, 1e-3));
    my $target_value = $mean + $sign * 3 * $std;

    # if std is very low (or zero), skip this test
    my $severity = $std > 1e-3 ? floor($zscore) : 0;

    my $rule = 'zscore247';
    if($sensor =~ /$nonscada_sensors/) {
      $rule .= '_nonscada';
    }
    if($sign < 0) {
      $rule .= "_negative";
    }

    if (($sign < 0 && $severity > .1) || $severity >= 2) {
      push(@ret, {
		  'station' => $station, 'sensor' => $sensor, 'reading' => $v, 'max_allowed' => $target_value, 'read_time' => $ts,
		  'detection_time' => $NOW, 'units' => $units, 'severity' => $severity, 'mean' => $mean, 'std' => $std,
		  rule => $rule});
    }

    # Check against various air-quality standards
    my $msg_d;
    $msg_d = hourly_warning_helper($station, $sensor, $v, \%warn_levels_eurostd, 'eurostd',
				   ['normal', 'low', 'medium', 'high', 'very high'],
				   [0,         1,    9,       30,     100],
				   $ts, $units);
    push(@ret, $msg_d) if(defined($msg_d));

    $msg_d = hourly_warning_helper($station, $sensor, $v, \%warn_levels_legal_daily, 'curr_daily_max',
				   ['normal','above standard daily mean value'],
				   [0, 210],
				   $ts, $units);
    push(@ret, $msg_d) if(defined($msg_d));

    $msg_d = hourly_warning_helper($station, $sensor, $v, \%warn_levels_legal_hourly, 'curr_hourly_max',
				   ['normal','above standard daily mean value'],
				   [0, 220],
				   $ts, $units,
				  );
    push(@ret, $msg_d) if(defined($msg_d));
    
    $msg_d = hourly_warning_helper($station, $sensor, $v, \%warn_levels_opacity_hourly, 'opacity_hourly',
				   ['normal','above warning value for opacity'],
				   [0, 220],
				   $ts, $units,
				  );
    push(@ret, $msg_d) if(defined($msg_d));
    
    # since we sorted by time, we can stop processing as soon as we see a reading outside the lookback window
    last if($ts < $NOW - $alert_lookback_hours*3600);
  }
  return \@ret;
}

sub hourly_warning_helper {
  my ($station, $sensor, $val, $warn_levels, $rulename, $warn_strings, $warn_severity, $ts, $units) = @_;
  my $ret;

  ($val, $units) = convert_units($station, $sensor, $val, $units);

  my ($warn_value, $warn_threshold) = warn_value($station, $sensor, $val, $warn_levels);
  if(defined($warn_value)) {
    my $warn_value_str = $warn_strings->[$warn_value];
    my $severity =       $warn_severity->[$warn_value];
    if ($severity >= 2) {
      $ret = {
        'station' => $station, 'sensor' => $sensor, 'reading' => $val, 'read_time' => $ts,
        'detection_time' => $NOW, 'units' => $units, 'severity' => $severity,
        'warn_threshold' => $warn_threshold,
        'warnlevel' => $warn_value_str,
        rule => $rulename};
      }
  }
  return $ret;
}

    
sub convert_units {
  my ($station, $sensor, $val, $units) = @_;
  if($units =~ m/PPB/i) {
    my $coefficient = $unit_coefficient{$sensor}; # BUGBUG: we should consider the altitude of the station. For now we assume all are at sea level
    if(!defined($coefficient) || $coefficient <= 0) {
      warn "Can't convert $sensor at $station";
    } else {
      $val *= $coefficient;
      $units = 'ug/m3 equivalent';
    }
  }
  return ($val, $units);
}

sub warn_value {
  my ($station, $sensor, $val, $warn_levels) = @_;
  my $fudge_factor = 1;
  my $levels;
  # first try the specific value for this particular sensor
  my $key = "$station:$sensor";
  $levels = $warn_levels->{$key};
  if(defined($levels)) {
    $fudge_factor = 2; # fudge factor double until I figure out the HEITER PLITA calculation
  } else { # if no specific value, try the per-pollutant value
    $levels = $warn_levels->{$sensor};
  }
  return undef unless(defined($levels));
  my @levels = sort { $a <=> $b } @$levels;
  my $i;
  for($i=scalar(@levels)-1; $i>=0; $i--) {
    if($val > ($fudge_factor * $levels[$i])) {
      last;
    }
  }
  return ($i+1, $levels[$i]);
}

sub dump_data {
  my ($data) = @_;
  # earliest date
  my $epoch = '20100101';
  
  $epoch = datetime_from_str($epoch);

  my ($fh, $filename) = tempfile();

  # header
  print $fh join("\t",
               'date',
               'year',
               'month',
               'day',
               'hour',
               'station',
               'sensor',
               'val',
               'epoch_days') . "\n";


  for my $station (keys %{$data}) {
    for my $sensor (keys %{$data->{$station}}) {
      my @reads = sort { $b <=> $a } keys %{$data->{$station}->{$sensor}->{'reads'}};
      for my $ts (@reads) {
        my $v = $data->{$station}->{$sensor}->{'reads'}->{$ts};
        my $read_ts = datetime_from_epoch($ts);
        my $delta_days = int(($read_ts->epoch() - $epoch->epoch())/(24*60*60));
        print $fh join("\t",
                     $read_ts->ymd(''),
                     $read_ts->year,
                     $read_ts->month,
                     $read_ts->day,
                     $read_ts->hour,
                     $station,
                     $sensor,
                     $v,
                     $delta_days,
            ) . "\n";
        last if($ts < $NOW - $alert_lookback_hours*3600);
      }
    }
  }
  close($fh);
  return $filename;
}

sub compute_AQI {
  my ($latest_data) = @_;
  my @ret = ();

  my $has_data = 0;
  # check that there'any data
  for my $station (keys %{$latest_data}) {
    for my $sensor (keys %{$latest_data->{$station}}) {
      $has_data = 1;
      next;
    }
    next if($has_data);
  }

  return unless($has_data);

  my ($risky_fh, $processed_data) = tempfile();
  close($risky_fh);

  my $data_dump = dump_data($latest_data);

  
  # prepare command line
  # ~levg/enviroment/calcAQI.sh /tmp/hourly.csv ~dpelleg/work/envmon/hourly_results.csv "2017-06-19 21:00" "2017-06-19 21:00"
  my $dt_now = strftime("%Y-%m-%d %H:%M", localtime($NOW));
 
  my @exec = ('/home/levg/enviroment/calcAQI.sh',
              $data_dump,
              $processed_data,
              $dt_now,
              $dt_now);

    if(system(@exec) == 0) {
    open(my $aqi, '<', $processed_data) or die "Could not open '$processed_data' $!\n";
    my $h = read_AQI($aqi);
    close($aqi);

    while (my ($station, $hh) = each %$h) {
      # transform from 100 (best) going down to -200, into the range: 50 (best) going up to 200
      my $aqi_val = $hh->{'AQI'};
      if(defined($aqi_val)) {
        if($aqi_val <= 0) {
          my $adjusted_AQI = 50 + (1/2)*(100 - $aqi_val);
          push(@ret, {
            'station' => $station, 'sensor' => $hh->{'AQI_sensor'},
            'AQI' => $aqi_val,
            'read_time' => $hh->{'date'},
            'detection_time' => $NOW,
            'severity' => $adjusted_AQI,
            'rule' => 'AQI'});
        }
      }
    }
  } else {
      print "system @exec failed: $?"
  }

  
  unlink($processed_data, $data_dump) or print "Could not unlink files: $!";
  
  return \@ret;
}
