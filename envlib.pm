package envlib;

use HTML::TableExtract;
use Encode;
use JSON;
use Time::Local;
use File::Find;
use POSIX;
use LWP::Simple;
use Net::SMTP;
use Data::Dumper;
use Email::Stuffer;
use Email::Sender::Transport::SMTP;
use DateTime;
use DateTime::TimeZone;
use Time::Out qw(timeout) ;

use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION     = 1.00;
@ISA         = qw(Exporter);
@EXPORT      = (qw/slurp_file_html slurp_subdir_html slurp_sensor_table_csv slurp_subdir_csv parse_sensor_table_multistation slurp_subdir_multistation slurp_file_multistation merge_two_tables ts_to_dow_tod my_format read_vals write_vals/,
		qw/load_msgdb store_msgdb load_userdb store_userdb send_sms send_email translate_station compute_daily_averages stdsqd_from_moments/,
		qw/insist string_to_timestamp annual_table datetime_from_str datetime_from_epoch/,
		qw/get_event_ts get_event_ts_em set_event_ts event_older_than_em event_older_than get_user_eventmap evmap_key cleanup_eventmap/,
		qw/read_AQI/,
		);

my $conf = load_db('db/conf.json');

my $user_db_fname = 'db/users';
my $pending_msgs_db_fname = 'db/pending_msgs';

my %xlat = ( 'שפרינצק' => 'Shprintzak',
             'רוממה' => 'Romema',
             'קריית ים' => 'Kiryat Yam',
             'קריית טבעון' => 'Kiryat Tivon',
             'קריית חיים-דגניה' => 'Kiryat Haim-Degania',
	     'קריית חיים - דגניה' => 'Kiryat Haim-Degania',
             # name changed from Degania to Regavim on 7 Aug 2016. Ran Minervi says that ever since early 2015 it was in Regavim school (same street as Degania school), and they remember to do the name change only in Aug 2016. 
             'קריית חיים-רגבים' => 'Kiryat Haim-Regavim',
             # name change 23 May 2017
             'קריית חיים' => 'Kiryat Haim-Regavim',
             'קריית ביאליק' => 'Kiryat Bialik',
	     'ק.ביאליק עופרים' => 'Kiryat Bialik Ofarim',
	     'ק.מוצקין בגין' => 'Kiryat Motzkin Begin',
             'קריית אתא' => 'Kiryat Ata',
             'נשר' => 'Nesher',
             'נווה שאנן' => 'Neve Shaanan',
             'נווה יוסף' => 'Neve Yosef',
             'כרמליה' => 'Carmelia',
             'כפר חסידים' => 'Kfar Hasidim',
             'יזרעאליה' => 'Yizraelia',
             'איינשטיין' => 'Einstein',
             'איגוד' => 'Igud (check-post)',
             'חיפה - איגוד' => 'Igud (check-post)', # new name as of Feb 7 2017
             'אחוזה' => 'Ahuza',
             'קריית מוצקין' => 'Kiryat Motzkin',
             'קריית בנימין' => 'Kiryat Binyamin',
             'דליית אל כרמל' => 'D.CARMEL',
             'שמן' => 'Shemen',
             'פז שמנים' => 'Paz Shmanim',
             'פז' => 'Paz',
             'דלק' => 'Delek',
             'סונול' => 'Sonol',
             'תשן נמל הדלק' => 'Tashan fuel port',
             'דור כימיקלים' => 'Dor chemicals',
             'Haifa g-40' => 'mahzam 40',
             'Haifa g-30' => 'mahzam 30',
             'שוק' => 'SHOOK',
             'BAZAN TO-1,2' => 'BAZAN TO-1 2', # name change around early May 2017
	     'הדר' => 'Hadar',		       # started Nov 2017
	     'אחוזה תחבורתית' => 'Ahuza transportation', # started Nov 2017
             'חוגים' => 'Hugim',			 # started Mar 2018
             'עצמאות חיפה' => 'Atzmaut',	 # started Mar 2018
             'ד.עכו - ק.מוצקין' => 'Kiryat Motzkin Acco road', 	 # started Mar 2018
             'ניידת 6' => 'Mobile 6',# started Mar 2018
             'ניידת 5' => 'Mobile 5',# started Mar 2018
             'ניידת 4' => 'Mobile 4',# started Apr 2019
             'פארק כרמל' => 'Park Carmel', #started Apr 2018
	     'מסופי ניפוק' => 'Masofei Nipuk', #started Aug 2018
    );

# we also add mapping form the UTF-8 encoded versions
{ 
  my @xlatk = keys %xlat;
  map {
    $xlat{encode_utf8($_)} = $xlat{$_};
  } @xlatk;
}

our $TZ_HERE = 'Asia/Jerusalem';

my @colors_html = ('006495', 'E0A025', 'F2635F', 'F4D00C',
	      '462066', '00AAA0', 'FF0000', '00FF00', '0000FF', '000000');
my $last_color_used_html = -1;
my $max_rect_width_html = 100;

# for the convenience of &wanted calls, including -eval statements:
use vars qw/*name *dir *prune/;
*name   = *File::Find::name;
*dir    = *File::Find::dir;
*prune  = *File::Find::prune;

sub parse_sensor_table_html {
  my ($html, $depth) = @_;
  my (@col_headers, @col_units);
  $depth = 2 unless defined($depth);
  my $te = HTML::TableExtract->new(depth => $depth);
  my %ret;
  $te->utf8_mode(1);
  $te->parse(($html));
  foreach my $row ($te->rows) {
    my @vals = map { s/^\s*// ; s/\s*$// ; $_ } @$row; # clean whitespace before and after
    if(!@col_headers) {		# first line is column headers
      @col_headers = @vals;
    } elsif(!@col_units) {		# second line is units
      @col_units = map { degreek($_) } @vals;
    } else {
      my $station = translate_station(shift(@vals));
      my $ts = shift(@vals);
      $ts = string_to_timestamp($ts);
      map {
        my $idx = $_;
        my $v = $vals[$idx];
        $v =~ s/\xA0//g;            # &nbsp; translates to A0
	my $sensor = $col_headers[$idx+2];
	my $units =  $col_units[$idx+2];
	$ret{$station}->{$sensor}->{'units'} = $units;
        if(!exists($ret{$station}->{$sensor}) || ($v ne "")) { # ensure each record has at least one observation
          $ret{$station}->{$sensor}->{'reads'} = { $ts => $v };
        }
      } 0..scalar(@vals)-1;
    }
  }
  return \%ret;
}

sub merge_two_tables {
  my ($t1, $t2) = @_;
  my %ret = defined($t1) ? %$t1 : ();
  
  map {
    my $station = $_;
    my $station_v = $t2->{$station};
    map {
      my $sensor = $_;
      my $sensor_v = $station_v->{$sensor};
      if(!defined($ret{$station}->{$sensor}->{'units'})) {
        $ret{$station}->{$sensor}->{'units'} = $sensor_v->{'units'};
      }
      # verify that the units match
      if($sensor_v->{'units'} eq $ret{$station}->{$sensor}->{'units'}) {
	map {
	  my $ts = $_;
	  my $v = $sensor_v->{'reads'}->{$ts};
	  my $ver = 7;
	  if(exists($ret{$station}->{$sensor}->{'version'}->{$ts})) { # update the version to the minimum value
	    $ver = min($ret{$station}->{$sensor}->{'version'}->{$ts}, $sensor_v->{'version'}->{$ts});
	  } else {
	    $ver = $sensor_v->{'version'}->{$ts};
	  }
	  $ret{$station}->{$sensor}->{'version'}->{$ts} = $ver;
	  $ret{$station}->{$sensor}->{'reads'}->{$ts} = $v;
	} keys %{$sensor_v->{'reads'}};
      } else {
	my $ignore;
	# ignore rules
	# Added 20 April 2016: new station as of 3 March 2016. Change is probably benign.
	$ignore = 1 if($station eq 'GADIV CTO' &&
		       $sensor eq 'BENZN' &&
		       $sensor_v->{'units'} =~ 'mg/(m3|hr)' &&
                       $ret{$station}->{$sensor}->{'units'} =~ 'mg/(hr|m3)');
	# Added 6 Nov 2016: another change of units in Gadiv CTO
	$ignore = 1 if($station =~ /GADIV CTO/ &&
		       $sensor eq 'BENZN' &&
		       $sensor_v->{'units'} =~ m!(gr|mg)/(hr|m3)! && # match may occur in either direction
                       $ret{$station}->{$sensor}->{'units'} =~ m!(gr|mg)/(hr|m3)!);
	# Added 10 Aug 2016: change of units in MAHZAM
	$ignore = 1 if($station =~ /mahzam [34]0/ &&
		       $sensor eq 'NOXEMISS' &&
		       $sensor_v->{'units'} =~ m!(Kg/Hr|mg/Nm3)! && # match may occur in either direction
                       $ret{$station}->{$sensor}->{'units'} =~ m!(Kg/Hr|mg/Nm3)!);
	# Added 24 Dec 2017: change of units in Moztkin/Hadar
	$ignore = 1 if($station =~ /(Kiryat Motzkin Begin)|(Hadar)$/ &&
		       $sensor eq 'BENZN' &&
		       $sensor_v->{'units'} =~ m!(PPB|ug/m3)! && # match may occur in either direction
                       $ret{$station}->{$sensor}->{'units'} =~ m!(PPB|ug/m3)!);
    # Added 19 Jan 2018 to deal with some data from 2016
	$ignore = 1 if($station eq 'Kiryat Haim-Degania' &&
		       $sensor eq 'BENZN' &&
		       $sensor_v->{'units'} =~ m!(mg|ug)/m3! &&
                   $ret{$station}->{$sensor}->{'units'} =~ m!(mg|ug)/m3!);
	# Added 17 Nov 2019, I'm not sure what is happening
	$ignore = 1 if($station eq 'Mobile_New' &&
		       $sensor eq 'O3' &&
		       $sensor_v->{'units'} =~ m!(mm|ug/m3)! &&
		       $ret{$station}->{$sensor}->{'units'} =~ m!(mm|ug/m3)!);
	# Added 29 Nov 2019
	$ignore = 1 if($station eq 'BAZAN TO-4' &&
		       $sensor eq 'Thermal_Oxydation Nox DRY' &&
		       $sensor_v->{'units'} =~ m!(mm|mg/m3)! &&
		       $ret{$station}->{$sensor}->{'units'} =~ m!(mm|ug/m3)!);
	# Added 29 Nov 2019
	$ignore = 1 if($station eq 'BAZAN TO-4' &&
		       $sensor =~ 'SO2S[34]' &&
		       $sensor_v->{'units'} =~ m!(mg/Nm3|Kg/Hr)! &&
		       $ret{$station}->{$sensor}->{'units'} =~ m!(mg/Nm3|Kg/Hr)!);

    if(!$ignore) {
          warn join(" ?-? ", $station, $sensor, $sensor_v->{'units'}, $ret{$station}->{$sensor}->{'units'}) . "\n";
        }
      }
    } keys %$station_v;
  } keys %$t2;

  return \%ret;
}

sub slurp_file_html {
  my ($fname, $depth) = @_;
  $depth = 2 unless defined($depth);
  my $ret = {};
  if(-f $fname) {
    open my $F, "$fname" or die;
    my @html = <$F>;
    $ret = parse_sensor_table_html(join("\n", @html), $depth);
    close($F);
  }
  return $ret;
}

sub slurp_subdir_html {
  my ($rootdir) = @_;
  my $ret = {};
  eval {
    find({ wanted => sub{my $d = slurp_file_html($_); $ret = merge_two_tables($ret, $d);}, no_chdir => 1}, $rootdir);
  }; warn $@ if $@;
  return $ret;
}

sub slurp_file_multistation {
  my ($fname) = @_;
  my $ret = {};
  my $version = 0;
  if(-f $fname) {
    if($fname =~ /(\d+)\.html(\.gz)?$/) {
      $version = $1;
    }

    # open by unzipping if needed
    my $F;
    if ($fname =~ /\.gz$/) {
      open($F, "gunzip -c $fname |") || die "can't open pipe to $fname";
    } else {
      open($F, $fname) || die "can't open $fname";
    }
    my @html = <$F>;
    $ret = parse_sensor_table_multistation(join("\n", @html), $version);
    close($F);
  }
  return $ret;
}

sub parse_sensor_table_multistation {
  my ($html, $version) = @_;

  my (@station_headers, @sensor_headers, @sensor_units);

  my %ret;
  my $te = HTML::TableExtract->new(depth => 0);
  $te->utf8_mode(1);
  $te->parse($html);
  foreach my $row ($te->rows) {
    my @vals = map { s/^\s*// ; s/\s*$// ; $_ = "" if($_ eq '--'); $_ } @$row; # clean whitespace before and after; clear "--" markers
    if(!@station_headers) {		# first line is station names
      @station_headers = map { translate_station($_) } @vals;
    } elsif(!@sensor_headers) {		# second line is sensor names
      @sensor_headers = @vals;
    } elsif(!@sensor_units) {		# third line is units
      @sensor_units = map { degreek($_)} @vals;
    } else {					    # data
      my $ts = shift(@vals);
      my $ts_before = $ts;
      next if($ts =~ /[a-zA-Z]/);	# there are some stats rows at the end
      $ts = string_to_timestamp($ts);
      map {
        my $idx = $_;
        my $v = $vals[$idx];
	my $sensor = $sensor_headers[$idx+1];
	my $units =  $sensor_units[$idx+1];
	my $station = $station_headers[$idx+1];
	$ret{$station}->{$sensor}->{'units'} = $units;
        if(!exists($ret{$station}->{$sensor}) || ($v ne "")) { # ensure each record has at least one observation
          $ret{$station}->{$sensor}->{'reads'}->{$ts} = $v;
	  $ret{$station}->{$sensor}->{'version'}->{$ts} = $version;
	}
	# print join("\t", encode_utf8($station), $sensor, $ts, $units, $v) . "\n";
      } 0..scalar(@vals)-1;
    }
  }
  return \%ret;
}

sub slurp_sensor_table_csv {
  my ($fname) = @_;

  my (@station_headers, @sensor_headers, @sensor_units);

  my %ret;
  open my $f, "<$fname" or die "$!: $fname";
  while(<$f>) {
    chomp;
    my @row = split(/,/, ($_));
    my @vals = map { s/^\s*// ; s/\s*$// ; $_ } @row; # clean whitespace before and after
    if(!@station_headers) {		# first line is station names
      @station_headers = map { translate_station($_) } @vals;
    } elsif(!@sensor_headers) {		# second line is sensor names
      @sensor_headers = @vals;
    } elsif(!@sensor_units) {		# third line is units
      @sensor_units = map { degreek($_)} @vals;
    } else {					    # data
      print STDERR join("  / ", @vals) . "\n";
      my $ts = pop(@vals);
      next if($ts =~ /[a-zA-Z]/);	# there are some stats rows at the end
      $ts = string_to_timestamp($ts);
      map {
        my $idx = $_;
        my $v = $vals[$idx];
	my $sensor = $sensor_headers[$idx];
	my $units =  $sensor_units[$idx];
	my $station = $station_headers[$idx];
	$ret{$station}->{$sensor}->{'units'} = $units;
	if(!exists($ret{$station}->{$sensor}) || ($v ne "")) { # ensure each record has at least one observation
	  $ret{$station}->{$sensor}->{'reads'}->{$ts} = $v;
	}
	# print join("\t", encode_utf8($station), $sensor, $ts, $units, $v) . "\n";
      } 0..scalar(@vals)-1;
    }
  }
  close($f);
  return \%ret;
}

sub slurp_subdir_multistation {
  my ($rootdir, $latest_file) = @_;
  my $ret = {};
  find({
	wanted => sub{
	  my $fname = $_;
	  my $skip = 0;
	  if(defined($latest_file)) {
	    if($fname =~ /(\d+)\.html(\.gz)?$/) {
	      my $fname_ts = $1;
	      if($fname_ts > $latest_file) {
		$skip = 1;
	      }
	    }
	  }

	  $skip = 1 if($fname =~ /\.DS_Store$/);
      
	  if(!$skip) {
	    my $d = slurp_file_multistation($fname);
	    $ret = merge_two_tables($ret, $d);
	  } else {
	    print STDERR "Skipping $fname\n";
	  }
	},
	no_chdir => 1
       },
       $rootdir);
  return $ret;
}

sub slurp_subdir_csv {
  my ($rootdir) = @_;
  my $ret = {};
  find({ wanted => sub{my $d = slurp_sensor_table_csv($_); $ret = merge_two_tables($ret, $d);}, no_chdir => 1}, $rootdir);
  #find({ wanted => sub{my $d = slurp_sensor_table_csv($_);}, no_chdir => 1}, $rootdir);
  return $ret;
}

my $tz_offset;			# without DST
# times reported by the monitoring system are always in winter clock (no DST). To adjust to the right local time, we treat them as UTC, and manually adjust by the timezone offset (in winter).
sub string_to_timestamp {
  my ($ts) = @_;
  if(!defined($tz_offset)) {
    my $tz = DateTime::TimeZone->new( name => $TZ_HERE );
    my $dt = DateTime->new(		# some date which is not in daylight saving (will probably fail for southern hemisphere)
			   year       => 1970,
			   month      => 1,
			   day        => 1);

    $tz_offset = $tz->offset_for_local_datetime($dt);
  }

  if($ts =~ m!(\d+)/(\d+)/(\d+) (\d+):(\d+)!) {
    my $jump_day_ahead;
    my ($mday, $mon, $year, $hour, $min) = ($1, $2, $3, $4, $5);
    if($hour == 24) {
      $hour = 0;
      $jump_day_ahead = 1;
    }
    my $dt = DateTime->new(
			   year       => $year,
			   month      => $mon,
			   day        => $mday,
			   hour       => $hour,
			   minute     => $min,
			   time_zone  => 'UTC'
			  );
    if($jump_day_ahead) {
      $dt->add(days => 1);
    }
    # adjust timezone offset (we don't want DST taken care of)
    $dt->subtract(seconds => $tz_offset);
    $ts = $dt->epoch;		# bring back the local offset
  }
  return $ts;
}

sub translate_station {
  my ($s) = @_;
  if(exists($xlat{$s})) {
    $s = $xlat{$s};
  }
  return $s;
}

sub degreek {                   # micrograms using a greek letter
  my ($s) = @_;
  $s = decode_utf8($s) ; $s =~ s/\xB5/u/g;
  return $s;
}

sub ts_to_dow_tod {
  my ($ts) = @_;
  my $tod = strftime('%H', localtime($ts));
  my $dow = 1 + ((strftime("%u", localtime($ts))) % 7); # move from Monday-based system (1 is Monday) to Sunday-based system (1 Sunday, 7 Saturday)
  return ($dow, $tod);
}

sub my_format {
  my ($s) = @_;
  if($s < 5) {
    return sprintf("%.1f", $s);
  }
  return sprintf("%.0f", $s);
}

sub read_vals {
  my $fname = shift(@_);
  my @ret = ();
  if(open(DATA, $fname)) {
    while(<DATA>) {
      chomp;
      unless($_ =~ /^\s*$/) {
        my $j = decode_json($_);
        push(@ret, $j);
      }
    }
    close(DATA);
  }
  return \@ret;
}

sub write_vals {
  my ($fname, $data) = @_;
  open(DATA, ">$fname") or die;
  map {
    print DATA encode_json($_) . "\n";
  } @$data;
  close(DATA);
}

sub load_userdb {
  return load_db($user_db_fname);
}

sub load_db {
  my ($fname) = @_;
  my $vals = read_vals($fname);
  my %ret;                      # we get back a list of hashes, put them all into a single hash
  for my $l (@$vals) {
    for my $user (keys %$l) {
      $ret{$user} = $l->{$user};
    }
  }
  return \%ret;
}

sub store_userdb {
  my ($db) = @_;
  # spread the hash keys into a list
  my @db;
  for my $user (keys %$db) {
    push(@db, { $user => $db->{$user} });
  }
  return write_vals($user_db_fname, \@db);
}

sub load_msgdb {
  return read_vals($pending_msgs_db_fname);
}

sub store_msgdb {
  my ($db) = @_;
  return write_vals($pending_msgs_db_fname, $db);
}

sub send_email {
  my ($recipient, $msg, $subject, $filename) = @_;
  my $success;

  my $script_owner = $conf->{'email'}->{'script_owner'};
  my $script_owner_name = $conf->{'email'}->{'script_owner_name'};
  my $mail_host = $conf->{'email'}->{'mail_host'};

  
  eval {
    unless(defined($subject)) {
      $subject = sprintf("Environmental sensor alert %s",
                         strftime("%d/%m/%y %H:%M", localtime(time())));
    }

    $sender = new Email::Stuffer
    {from => "$script_owner_name <$script_owner>",
     transport => Email::Sender::Transport::SMTP->new({host => $mail_host}),
     to => $recipient,
     subject => $subject,
     text_body => $msg,
        };
    if(defined($filename)) {
      map {
        $sender->attach_file($_);
      } @$filename;
    }
    $success = $sender->send();
  };
  if ($@ || !$success) {
    warn "Failed to send the message: $@\n";
    return undef;
  }
  return 1;
}

sub send_email_old {
  my ($recipient, $msg, $subject, $filename) = @_;
  eval {
    unless(defined($subject)) {
      $subject = sprintf("Environmental sensor alert %s",
                         strftime("%d/%m/%y %H:%M", localtime(time())));
    }

    $sender = new Mail::Sender
    {smtp => $mail_host, fake_from => $script_owner, from => "$script_owner_name <$script_owner>"};
    my %params = (to => $recipient,
                  subject => $subject,
                  msg => $msg,
        );
    if(defined($filename)) {
      $params{'file'} = $filename;
      $sender->MailFile(\%params);
    } else {
      $sender->MailMsg(\%params);
    }
  };
  if ($@) {
    warn "Failed to send the message: $@\n";
    return undef;
  }
  return 1;
}

sub send_email_older {
  my ($recipient, $msg, $subject) = @_;
  my $smtp = Net::SMTP->new($mail_host);
  if(!$smtp) {
    warn("Cannot connect to mail host $mail_host");
    return 0;
  }
  $smtp->mail($script_owner);
  $smtp->to($recipient);
  
  unless(defined($subject)) {
    $subject = sprintf("Environmental sensor alert %s",
                       strftime("%d/%m/%y %H:%M", localtime(time())));
  }

  my $msg_ = "From: $script_owner_name <$script_owner>\n";
  $msg_ .= "To: <$recipient>\n";
  $msg_ .= "Subject: $subject\n\n";
  $smtp->data();
  $smtp->datasend($msg_);
  $smtp->datasend($msg);
  $smtp->dataend();
  $smtp->quit;
  return 1;
}

sub send_sms {
  my ($recipient, $msg) = @_;
  $msg =~ s/\t/\n/g;
  $ua = LWP::UserAgent->new;
  $ua->agent("PSMS/0.1");
  my $url = 'http://www.smartsms.co.il/member/http_sms_xml_api.php?function=singles';
  my $reference = time();
  my $smartsms_user = $conf->{'sms'}->{'smartsms_user'};
  my $smartsms_password = $conf->{'sms'}->{'smartsms_password'};
  my $smartsms_source_number = $conf->{'sms'}->{'smartsms_source_number'};

  my $xmlstr = qq{<Request>
<UserName>$smartsms_user</UserName>
<Password>$smartsms_password</Password>
<Time>0</Time>
<Singles>
<Single>
<Message>$msg</Message>
<DestinationNumber>$recipient</DestinationNumber>
<SourceNumber>$smartsms_source_number</SourceNumber>
<ClientReference>$reference</ClientReference>
</Single>
</Singles>
</Request>
};

  my $req = HTTP::Request->new(POST => $url);
  $req->content_type('application/x-www-form-urlencoded');
  $req->content('xml='. url_encode($xmlstr));
  # Pass request to the user agent and get a response back
  my $res = $ua->request($req);

  # Check the outcome of the response
  my $success = 0;
  my $ret;
  if ($res->is_success) {
    if($res->content =~ m!<ErrorCode>(\w+)</ErrorCode>!) {
      $ret = $1;
    } elsif($res->content =~ m!<Response><SinglesResults><SingleResult><ServerId>(\w+)</ServerId><ClientReference>(\w+)</ClientReference></SingleResult></SinglesResults></Response>!) {
      $success = 1;
      $ret = "$1:$2";
    }
  }
  # print "$success $ret\n";
  return ($success, $ret);
}

sub url_encode {
  my $text = shift;
  $text =~ s/([^a-z0-9_.!~*'(  ) -])/sprintf "%%%02X", ord($1)/egi;
  $text =~ tr/[ ]/+/;
  return $text;
}

sub compute_daily_averages {
  my ($data) = @_;
  my $tab;			# intermediate table
  my $ret;

  # scan data and tabulate
  for my $station (keys %$data) {
    my $h = $data->{$station};
    for my $sensor (keys %$h) {
      my $hh = $h->{$sensor};
      my $cell = $tab->{$station}->{$sensor};
      for my $ts (keys %{$hh->{'reads'}}) {
        my $val = $hh->{'reads'}->{$ts};
	my $dt = DateTime->from_epoch(epoch => $ts, time_zone => 'floating');
	my $date = $dt->ymd('');
	$tab->{$station}->{$sensor}->{$date}  = {} unless exists($tab->{$station}->{$sensor}->{$date});

	my $microcell =  $tab->{$station}->{$sensor}->{$date};
	$microcell->{'n'}++;
	$microcell->{'sum'} += $val;
	$microcell->{'sumsqd'} += $val*$val;
      }
    }
  }

  # calculate daily averages and invert the main key
  for my $station (keys %$tab) {
    for my $sensor (keys %{$tab->{$station}}) {
      for my $date (sort keys %{$tab->{$station}->{$sensor}}) {
	my $slice = $tab->{$station}->{$sensor}->{$date};
	my $avg = $slice->{'sum'} / $slice->{'n'};
	$ret->{$date}->{$station}->{$sensor} = $avg;
      }
    }
  }

  return $ret;
}

sub stdsqd_from_moments {
  my ($N, $sum, $sumsqd) = @_;
  my $stdqd;
  my $mean = $sum/$N;
  if ($N > 1) {	  
    $stdsqd = ($N*$sumsqd - ($mean ** 2))/($N*($N-1));
  } else {
    $stdsqd = ($N*$sumsqd - ($mean ** 2))/($N*($N)); # hehe, zero
  }
  return $stdsqd;
}

sub insist {
  my ($fun, @args) = @_;
  my $ret;
  my $success;
  for(my $retries = 1; $retries < 5 && !$success; $retries++) {
    eval {
      timeout 60, @_ => sub { $ret = &$fun(@args); };
    };
    if ($@){
      print STDERR "TIMEOUT: $@\n";
      sleep(5);
    } else {
      $success = 1;
    }
  }
  return $ret;
}


sub annual_table {
  my ($year, $events) = @_;
  my %legend;
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
	  my $color = get_color_html($evtype, \%legend);
	  $in_cell .= rect_html("#" . $color, $evcount);
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

  my $legend_out = "";
  for my $evtype (sort keys %legend) {
    my $col = $legend{$evtype};
    $legend_out .= rect_html($col) . $evtype . "&nbsp;&nbsp;&nbsp;";
  };

  $legend_out .= "<br></br>\n";

  return ($ret, $legend_out);
}

sub rect_html {
  my ($color, $width) = @_;
  if(defined($width)) {
    $width *= 10;
  } else {
    $width = 30;
  }
  $width = $max_rect_width_html if($width > $max_rect_width_html);
  my $height = 5;
  my $el = 'div';
  my $ret = sprintf('<%s style="width:%dpx;height:%dpx;border:0px; background-color:%s;"></%s>', $el, $width, $height, $color, $el);
  return $ret;
}
    
sub get_color_html {
  my ($event_type, $legend) = @_;
  if(exists($legend->{$event_type})) {
    return $legend->{$event_type};
  }
  my $col_idx = ++$last_color_used_html % scalar(@colors_html);
  $legend->{$event_type} = $colors_html[$col_idx];
  return $legend->{$event_type};
}

# manage the last-sent event db

sub get_user_eventmap {
  my ($users, $user) = @_;
  my $ret = $users->{$user}->{'last_sent'};
  if(!defined($ret)) {
    $users->{$user}->{'last_sent'} = {};
  }
  return $users->{$user}->{'last_sent'};
}

sub get_event_ts {
  my ($users, $user, $key) = @_;
  my $evmap = get_user_eventmap($users, $user);
  return get_event_ts_em($evmap, $key);
}

sub get_event_ts_em {
  my ($em, $key) = @_;
  return $em->{$key};
}

sub set_event_ts {
  my ($users, $user, $key) = @_;
  my $evmap = get_user_eventmap($users, $user);
  $evmap->{$key} = time();
}

# check if the recorded event is older given period (if it happened more than that much time ago, or else if it never happened)
sub event_older_than {
  my ($users, $user, $key, $window) = @_;
  my $evmap = get_user_eventmap($users, $user);
  return event_older_than_em($evmap, $key, $window);
}

sub event_older_than_em {
  my ($em, $key, $window) = @_;
  my $ts = get_event_ts_em($em, $key);
  return 1 if(!defined($ts));
  return ($ts < (time() - $window));
}

sub evmap_key {
  my ($r, $station, $sensor) = @_;
  return join('?', $r, $station, $sensor);
}

sub cleanup_eventmap {
  my ($users) = @_;
  while (my ($user, $v) = each %$users) {
    my $evmap = get_user_eventmap($users, $user);
    foreach my $ev (keys %$evmap) { # remove entries generated by evmap_key
      if($ev =~ /\?/) {
	delete($evmap->{$ev});
      }
    }
    # remove old cruft
    foreach my $k (qw/last_send last_send_sms/) {
      delete($users->{$user}->{$k});
    }
  }
}

sub min {
  my ($x, $y) = @_;
  return $y unless(defined($x));
  return $x unless(defined($y));
  return $x < $y ? $x : $y;
}

sub read_AQI {
  my ($data) = @_;
  my %ret;
  my @colnames;			# map index on the line to its column name
  while(<$data>) {
    chomp;
    my @line = split(/,/, $_);
    if(!(@colnames)) {	# header line
      for my $i (0..$#line) {
	$colnames[$i] = $line[$i];
      }
    } else {
      my $key = $line[0];
      for my $i (1..$#line) {
	my $v = $line[$i];
	if($colnames[$i] eq 'date') {
	  $v = string_to_timestamp($v);
	}
	$ret{$key}->{$colnames[$i]} = $v;
      }
    }
  }
  return \%ret;
}

sub datetime_from_str {
  my ($date) = @_;
  my $ret;
  if($date =~ m!(\d{4})(\d{2})(\d{2})$!) {
    $ret = DateTime->new(
			 year       => $1,
			 month      => $2,
			 day        => $3,
			 time_zone  => $TZ_HERE,
			);
  }
  return $ret;
}

sub datetime_from_epoch {
  my ($ts) = @_;
  return DateTime->from_epoch(epoch => $ts, time_zone => $TZ_HERE);
}

1;
