#!/usr/local/bin/perl5.24.3 -w

# process pending message queue

use envlib;
use POSIX;
use WWW::Mechanize;
use WWW::Mechanize::PhantomJS;
use File::Path qw/make_path/;
use Time::Local;
use File::Temp qw/ tempfile tempdir /;
use Data::Dumper;
use List::Util qw(first max maxstr min minstr reduce shuffle sum);
use Encode;
use Getopt::Std;

my $MAX_MAIL_NOTIFICATIONS = 50; # maximum number of distinct alerts in a long-format message (mail)
my $MAX_MAIL_CHARTS = 10; # maximum number of chart pairs to attach to the mail

# the logic for SMS alerts is that EITHER condition meets, while
# the logic for email alerts is that BOTH condition meet
my $MIN_NSENSORS_SMS_ALERT = 5;      # minimum number of distinct sensors to trigger a short-format message (SMS)
my $MIN_NSTATIONS_SMS_ALERT = 5;     # minimum number of distinct stations to trigger a short-format message (SMS)

my $MIN_NSENSORS_MAIL_ALERT = 1;      # minimum number of distinct sensors to trigger a short-format message (email)
my $MIN_NSTATIONS_MAIL_ALERT = 1;     # minimum number of distinct stations to trigger a short-format message (email)

my $HOUR_DAILY_RUNDOWN = 20;    # by this time of day (hour in 24-hour format), email messages are sent even if the mail alert logic doesn't otherwise hit

my $MINUTE=60;
my $HOUR=60*$MINUTE;

my $MIN_SEND_HIATUS = 1*$HOUR - 5*$MINUTE;   # time in seconds to wait before sending consecutive messages. We take off five minutes to prevent a race condition in case the cron entry is every round number of hours
my $MIN_SEND_SMS_HIATUS = 24*$HOUR - 5*$MINUTE;   # time in seconds to wait before sending consecutive SMS messages.

my $MSG_EXPIRY = 24*$HOUR;       # time in seconds, after which a message should not be sent anymore

my $OUTDIR_BASE = tempdir( CLEANUP => 1 ); # will be auto-cleaned on exit

# minimum time in seconds between consecutive messages about the same rule/station/sensor
my %resend_hiatus = (
  'zscore247' => 10*$HOUR,
  'zscore247_nonscada' => 10*$HOUR,
  'missing_data' => 48*$HOUR,
  'missing_lots_data' => 24*$HOUR,
  'eurostd' => 6*$HOUR,
  'curr_daily_max' => 4*$HOUR,
  'curr_hourly_max' => 4*$HOUR,
  'zscore_cumulative' => 10*$HOUR,
  'zscore_cumulative_nonscada' => 10*$HOUR,
  'legal_max_daily' => 6*$HOUR,
  'AQI' => 6*$HOUR,
  'state_change' => 1*$HOUR,
		    );

my %xlat_sensor = (
		   'חנקן חד-חמצני' => 'NO',
		   'חנקן דו-חמצני' => 'NO2',
		   'אוזון' => 'O3',
		   'חלקיקי 10 מיקרון' => 'PM10',
		  );

my %periodmap = (
		 'daily' => {'days_back' => 0, 'button' => 'RadioButtonList2_0'},
		 'weekly' => {'days_back' => 6, 'button' => 'RadioButtonList2_1'},
		 );

getopts("n:", \%options);

my $NOW = $options{'n'};

if(!defined($NOW)) {
  $NOW = time();
} elsif($NOW =~ m!(\d+)/(\d+)/(\d+) (\d+):(\d+)!) {
  my ($mday, $mon, $year, $hour, $min) = ($1, $2, $3, $4, $5);
  $NOW = timelocal(0, $min, $hour, $mday, $mon-1, $year);
} else {
  die "Unknown time format: $NOW";
}

my $users = load_userdb();

if (!defined($users) || (scalar(keys %$users) == 0)) {
  $users = { 'ex' => {'Name' => 'John Doe', 'phone' => '055-5555555', 'email' => 'my_envalert@mydomain.example', 'paniclevel' => 3}};
  store_userdb($users);
}

# periodic database cleanup in the morning (will revive some of the stuff waiting from last night)
my $dt_now = DateTime->from_epoch(epoch => $NOW, time_zone => $envlib::TZ_HERE);
if($dt_now->hour() == 6) {
  cleanup_eventmap($users);
}

my $mq_pre = load_msgdb();
my @mq_post = ();               # messages remaining to send next time

# aggregate messages by user, and also de-dupe by message content
my %msgs_peruser = ();
for my $msg (@$mq_pre) {
  my $recipient = $msg->{'recipient'};
  my $rule = $msg->{'rule'};
  my $dedupe_key = join(":", map { $msg->{$_} } qw/station sensor read_time rule/);
  if(defined($rule) && ($rule =~ /^(zscore_cumulative|legal_max_daily|missing_data|missing_lots_data|curr_daily_max)/)) {
    $dedupe_key = join(":", map { $msg->{$_} } qw/station sensor rule/);
  }
  # if there's a dupe, pick the one that's latest
  if(exists($msgs_peruser{$recipient}->{$dedupe_key})) {
    my $curr_msg = $msgs_peruser{$recipient}->{$dedupe_key};
    if($msg->{'read_time'} > $curr_msg->{'read_time'}) {
      $msgs_peruser{$recipient}->{$dedupe_key} = $msg;
    }
  } else {
    $msgs_peruser{$recipient}->{$dedupe_key} = $msg;
  }
}

# scan users with pending messages and attempt to send
for my $user (keys %msgs_peruser) {
  my @raw_messages = map { $msgs_peruser{$user}->{$_} } keys %{$msgs_peruser{$user}};
  my $send_success = 0;

  my ($messages, $dormant, $distinct, $aux) = typeset_messages(\@raw_messages,
							       get_user_eventmap($users, $user),
							       $users->{$user}->{'paniclevel'},
							       $users->{$user}->{'eventmask'}
							      );

  # check enough time had passed since the last messaging
  if((scalar(@$messages) > 0) && event_older_than($users, $user, 'mail', $MIN_SEND_HIATUS)) { 
    # attempt to send
    my $attempt_to_send = 0;
    my ($n_stations_seen, $n_sensors_seen, $microsummary) = summary_helper($distinct->{'all'});
    my $all_txt = join("\n", map { sprintf("%s (%s)", $_->{'txt'}, $_->{'rule'}) } @$messages);
    if(defined($aux->{'url'})) {
      $all_txt .= "\n\nFull information at : " . $aux->{'url'} . "\n\n";
    }

    my $mail_subject = sprintf("Enviromental report: %s", $microsummary);

    my ($n_stations_seen_short, $n_sensors_seen_short, $microsummary_short) = summary_helper($distinct->{'short'});
    my $short_txt = sprintf("Pollution event, %s", $microsummary_short);
    
    if($n_sensors_seen_short >= $MIN_NSENSORS_SMS_ALERT || $n_stations_seen_short >= $MIN_NSTATIONS_SMS_ALERT) {
      if(defined($users->{$user}->{'phone'}) &&
         event_older_than($users, $user, 'sms', $MIN_SEND_SMS_HIATUS)) {
        my ($send_sms_success, $send_sms_diag) = send_msg_sms($users->{$user}->{'phone'}, $short_txt);
        $send_success ||= $send_sms_success;
        if(!$send_sms_success && $send_sms_diag) {
          warn $send_sms_diag;
        } else {
	  set_event_ts($users, $user, 'sms');
	}
      }
    }

    if(defined($users->{$user}->{'email'})) {
      my $hour_now = strftime("%H", localtime($NOW));

      # apply logic of flushing any remaining messages by end of day
      if($hour_now >= $HOUR_DAILY_RUNDOWN) {
        $attempt_to_send ||= 1;
      }

      # apply logic of having enough types of messages
      if($n_sensors_seen >= $MIN_NSENSORS_MAIL_ALERT && $n_stations_seen >= $MIN_NSTATIONS_MAIL_ALERT) {
        $attempt_to_send ||= 1;
      }

      if($attempt_to_send) {
        my $email_success = send_msg_email($users->{$user}->{'email'}, $all_txt, $mail_subject, $aux->{'graph_file'});
        $send_success ||= $email_success;        # BUGBUG: need to keep a different field for last-sent by SMS
      }
    }

    if($attempt_to_send) {
      if($send_success) {           # update last-send time
	# whole mail
	set_event_ts($users, $user, 'mail');
	# individual events
	map {
	  my $m = $_;
	  $key = evmap_key(map { $m->{$_} } qw/rule station sensor/);
	  set_event_ts($users, $user, $key);
	} @$messages
      } else {                      # put messages back on queue for another attempt later
        warn "Failed to send to $user";
      }
    }
  }
  
  if(!$send_success) {                      # did not message to the user for any reason, retain the messages in the queue
    push(@mq_post, @$messages);
  }
  push(@mq_post, @$dormant);	# keep in the queue the messages that might be sent in the future
}

store_msgdb(\@mq_post);
store_userdb($users);

sub send_msg_sms {
  my ($phone, $msg) = @_;
  return send_sms($phone, $msg);
}

sub summary_helper {
  my ($h) = @_;
  my $distinct_stations = $h->{'stations'};
  my $distinct_sensors = $h->{'sensors'};
  my $n_stations_seen = scalar @$distinct_stations;
  my $n_sensors_seen = scalar  @$distinct_sensors;

  my $microsummary = sprintf("station%s: %s sensor%s: %s",
                             plural($n_stations_seen), join(",", @$distinct_stations),
                             plural($n_sensors_seen), join(",", @$distinct_sensors));
  return ($n_stations_seen, $n_sensors_seen, $microsummary);
}

sub send_msg_email {
  my ($email, $msg, $subject, $filename) = @_;
  my $ret = send_email($email, $msg, $subject, $filename);
  return $ret;
}

sub plural {
  my ($str) = @_;
  $str != 1 ? "s" : "";         # zero is plural
}

sub typeset_messages {
  my ($messages, $user_evmap, $paniclevel, $user_event_mask) = @_;
  my @messages = @$messages;
  my @dormant = ();

  # we should really get this from the HTML at the raw-processing stage
  my %station_id  = (
		   'Shprintzak' => 10,
		   'Romema' => 36,
		   'Kiryat Yam' => 9,
		   'Kiryat Tivon' => 7,
		   'Kiryat Haim-Degania' => 44,
		   'Kiryat Bialik' => 12,
		   'Kiryat Ata' => 3,
		   'Nesher' => 2,
		   'Neve Shaanan' => 1,
		   'Neve Yosef' => 37,
		   'Carmelia' => 38,
		   'Kfar Hasidim' => 13,
		   'Yizraelia' => 35,
		   'Einstein' => 5,
		   'Igud (check-post)' => 24,
		   'Ahuza' => 8,
		   'Kiryat Motzkin' => 11,
		   'Kiryat Binyamin' => 31,
		   'D.CARMEL' => 102,
);

  # sort by severity (higher first) and then reading time, most recent first
  my @sorted = sort {
    my $sa = $messages[$a]->{'severity'};
    my $sb = $messages[$b]->{'severity'};
    ($sa == $sb) ? 
      $messages[$b]->{'read_time'} <=> $messages[$a]->{'read_time'} :
      $sb <=> $sa;
    } 0..$#messages;
  @messages = @messages[@sorted];

  # remove irrelevant messages:
  # 1. messages that waited too long in the queue
  @messages = grep { $_->{'read_time'} > ($NOW - $MSG_EXPIRY) } @messages;

  # 2. messages from readings before the last send time to the user (since alert.pl goes back and scans a range of timestamps, it might double-submit a message to the queue)
  # Note that send time is taken per message type - there could have been a previous mail after the reading, but it might have skipped this type since it was still within the hiatus
  @messages = grep {
    my $m = $_;
    my $r = $m->{'rule'};
    my $station = $m->{'station'};
    my $sensor = $m->{'sensor'};
    my $keep = 1;
    my $key = evmap_key($r, $station, $sensor);
    my $last_send = get_event_ts_em($user_evmap, $key);
    if(defined($last_send)) {
      $keep = 0 if($m->{'read_time'} < $last_send);
    }
    $keep;
  } @messages;

  # 3. messages with severity below the user's panic level
  @messages = grep { $_->{'severity'} >= $paniclevel } @messages;

  # 4. Apply per-user mask
  my %evs;
  my %evs_exclude;
  my $mask_wantall;

  if(defined($user_event_mask)) {
    # represent as hash
    my @evs = split(',', $user_event_mask);
    map {
      my $ev = $_;
      if ($ev =~ /^-(.*)/) {
        $evs_exclude{$1} = 1;
      } elsif($ev eq 'all') {
        $mask_wantall = 1;
      } else {
        $evs{$ev} = 1;
      }
    } @evs;
  } else {
    $mask_wantall = 1;
  }

  @messages = grep {
    my $m = $_;
    my $r = $m->{'rule'};
    my $keep = 0;
    if(($mask_wantall || defined($evs{$r})) && !defined($evs_exclude{$r})) {
      $keep = 1;
    }
    $keep;
  } @messages;

  # 5. Apply per-rule hiatus - kept track of by station/sensor
  @messages = grep {
    my $m = $_;
    my $r = $m->{'rule'};
    my $station = $m->{'station'};
    my $sensor = $m->{'sensor'};
    my $keep = 1;
    my $key = evmap_key($r, $station, $sensor);
    if(defined($resend_hiatus{$r})) {
      $keep = 0 if(!event_older_than_em($user_evmap, $key, $resend_hiatus{$r}-5*$MINUTE));
    }
    if(!$keep) {
      push(@dormant, $m);	# if fail, make sure we retain it for when the time comes
    }
    $keep;
  } @messages;

  # limit quantity
  my @capped_messages = splice(@messages, 0, $MAX_MAIL_NOTIFICATIONS);
  # keep the ones left out, maybe they'll have room next time
  push(@dormant, @messages);

  @messages = @capped_messages;

  # count number of distinct sensors and distinct stations
  my %distinct;
  $distinct{'all'} = distinct_in_msgs(\@messages);
  $distinct{'short'} = distinct_in_msgs([ grep {$_->{'rule'} !~ /missing.*data/} @messages ]);
  
  # build a URL pointing to the station with the first message
  my $url;

  # generate charts to attach
  my @graphs;        # list of file names
  my %graphs_seen;			# we keep it as a hash (value is constant 1) to de-dupe
  for my $idx (0..$#messages) {
    my $this_station = $messages[$idx]->{'station'};
    my $this_sensor_group = $messages[$idx]->{'sensor'};
    if(!defined($url) && exists($station_id{$this_station})) {
      $url = sprintf('http://www.envihaifa.net/Online.aspx?ST_ID=%s%%3b', $station_id{$this_station});
    }
    my $todays_date = DateTime->from_epoch(epoch => $NOW)->dmy('/');
    my $group_graph = fetch_sensorgroup($todays_date, $this_sensor_group);
    if(defined($group_graph) && !exists($graphs_seen{$group_graph})) {
      push(@graphs, $group_graph);
      $graphs_seen{$group_graph} = 1;
    }

    my $rule = $messages[$idx]->{'rule'};
    my $period = 'daily';
    $period = 'weekly' if($rule eq 'zscore_cumulative');
    my $sensor_graph = fetch_sensor($todays_date, $this_station, $this_sensor_group, $period);
    if(defined($sensor_graph) && !exists($graphs_seen{$sensor_graph})) {
      push(@graphs, $sensor_graph);
      $graphs_seen{$sensor_graph} = 1;
    }
    last if(scalar(keys %graphs_seen) > $MAX_MAIL_CHARTS);
  }
  
  if(!defined($url)) {
    $url = 'http://www.envihaifa.net/Default.rtl.aspx';
  }

  my %aux = ('url' => $url, 'graph_file' => \@graphs );
  return (\@messages, \@dormant, \%distinct, \%aux);
}

sub distinct_in_msgs {
  my ($messages) = @_;
  my %ret;
  my (@distinct_sensors, @distinct_stations);
  {
    my (%stations_seen, %sensors_seen);
    map {
      my $msg = $_;
      $stations_seen{$msg->{'station'}}++;
      $sensors_seen{$msg->{'sensor'}}++;
    } @$messages;

    @distinct_stations = sort keys %stations_seen;
    @distinct_sensors = sort keys %sensors_seen;
  }

  $ret{'stations'} = \@distinct_stations;
  $ret{'sensors'} = \@distinct_sensors;

  return \%ret;
}

# download environmental sensor data: get the "group graph" as a PNG file

sub fetch_sensorgroup {
  my (@args) = @_;
  return insist(\&fetch_sensorgroup_internal, @args);
}

sub fetch_sensorgroup_internal {
  my ($date, $sensor_group) = @_;
  my $date_nextday;

  my ($outdir, $outfile, $outpath);
  if ($date =~ m!^(\d+)/(\d+)/(\d+)$!) {
    my $dt = DateTime->new(
			   year       => $3,
			   month      => $2,
			   day        => $1);
    my $dt_nextday = $dt->clone();
    $dt_nextday->add(days => 1);
    $date_nextday = $dt_nextday->dmy('/');
    # figure out output directory and filename
    $outfile = sprintf("%s_%s.png", $dt->strftime("%Y%m%d%H%M%S"), $sensor_group);
    $outdir = sprintf("%s/", $OUTDIR_BASE);
    $outpath = sprintf("%s%s", $outdir, $outfile);
  } else {
    warn "Bad date format $date\n";
    return undef;
  }

  if(-f $outpath) {             # output already exists from a previous run
    return $outpath;
  }
  # print STDERR "$date $date_nextday\n";
  
  my $mech = WWW::Mechanize::PhantomJS->new();

  $mech->get('http://www.envihaifa.net/GroupDialog.aspx');
  return undef unless($mech->success());

  my $driver= $mech->driver();
  
  $mech->form_id('form1');

  # choose output format: "graph"
  $mech->click_button(id => 'RadioButtonList1_1');
  # choose time range type: "interval"
  $mech->click_button(id => 'RadioButtonList2_3');

  # sensor group options: SO2 NOX PM10 O3 PM2.5 NO2 Butadiene 1-3 BENZN Etilbenzene CO NOx all bazan Gadiv_NOx
  $sensor_group = 'NOX' if($sensor_group eq 'NO');
  $sensor_group = 'Etilbenzene' if($sensor_group eq 'EthylB');
  my $sensor_group_key;
  if($mech->content() =~ m!<select \s*name\s*=\s*"ddlGroup"(.*?)</select>!s) {
    my $groupsel = $1;
    my %groupkey;
    while($groupsel =~ m!value="(\w+)">\s*(\S+)\s*</option>!g) {
      my ($key, $value) = ($1, $2);
      $groupkey{$value} = $key;
    }
    $sensor_group_key = $groupkey{$sensor_group};
  } else {
    warn "Bad form";
    return undef;
  }

  unless(defined($sensor_group_key)) {
    # warn "Unknown group $sensor_group";
    return undef;
  }
  
  $mech->field('ddlGroup', $sensor_group_key);

  sleep(1);
  # Date
  # start date
  my $f = $mech->by_id('BasicDatePicker1_TextBox', single => 1, frames => 1);
  #$mech->click_button(id => 'BasicDatePicker1'); # no idea why this is needed, but it is
  $mech->field($f, $date);
  # end date
  $f = $mech->by_id('BasicDatePicker2_TextBox', single => 1, frames => 1);
  #$mech->click_button(id => 'BasicDatePicker2'); # no idea why this is needed, but it is
  $mech->field($f, $date_nextday);
  sleep(1);
  
  # reporting statistic: average
  $mech->field('ddlType', 'ממוצע');
  sleep(1);
  
  # reporting frequency: hourly
  $mech->field('ddlTimeBase', '60');
  sleep(1);

  $mech->click('btnGenerateReport');
  return undef unless($mech->success());

  if ($mech->content =~ m!<img id="C1WebChart1"!) {
    if(! -d $outdir) {
      make_path($outdir) or die "Can't create path $outdir";
    }
    $driver->capture_screenshot($outpath) or die "Can't save screenshot $outpath";
  } else {
    return undef;
  }
  return $outpath;
}

# download environmental sensor data: get the daily/weekly report of a given station/sensor as a PNG file
sub fetch_sensor {
  my (@args) = @_;
  return insist(\&fetch_sensor_internal, @args);
}

sub fetch_sensor_internal {
  my ($date, $station, $sensor, $period) = @_;
  my $graph_start_date;

  $period = 'daily' unless(defined($period));
  die "Unknown period $period" unless(exists($periodmap{$period}));
  my ($outdir, $outfile, $outpath);
  if ($date =~ m!^(\d+)/(\d+)/(\d+)$!) {
    my $dt = DateTime->new(
			   year       => $3,
			   month      => $2,
			   day        => $1);
    $graph_start_date = $dt->clone();
    $graph_start_date->subtract(days => $periodmap{$period}->{'days_back'});

    # figure out output directory and filename
    $outfile = sprintf("%s_%s_%s_%s.png", $dt->strftime("%Y%m%d%H%M%S"), $station, $sensor, $period);
    $outdir = sprintf("%s/", $OUTDIR_BASE);
    $outpath = sprintf("%s%s", $outdir, $outfile);
  } else {
    warn "Bad date format $date\n";
    return undef;
  }

  if(-f $outpath) {             # output already exists from a previous run
    return $outpath;
  }
  # print STDERR "$date $date_nextday\n";
  
  my $mech = WWW::Mechanize::PhantomJS->new();  # 'launch_exe' => 'bin/phantomjs');

  $mech->get('http://www.envihaifa.net/frmStationReport.aspx');
  return undef unless($mech->success());

  my $driver= $mech->driver();
  
  $mech->form_id('form1');

  # choose output format: "graph"
  $mech->click_button(id => 'RadioButtonList1_1');
  # choose time range type: "daily"/"weekly"/etc
  $mech->click_button(id => $periodmap{$period}->{'button'});

  # Date
  # start date
  $mech->click_button(id => 'BasicDatePicker1'); # no idea why this is needed, but it is
  sleep(1);
  my $f = $mech->by_id('BasicDatePicker1_TextBox', single => 1, frames => 1);
  $mech->field($f, $graph_start_date->dmy('/'));
  # no need for end date
  
  # reporting statistic: average
  $mech->field('ddlAvgType', 'ממוצע');
  sleep(1);


  my $station_map = get_select_map('ddlStation', encode_utf8($mech->content()));
  my $station_key = $station_map->{$station};
  unless(defined($station_key)) {
    my $inhibit;           # some stations are not in the database for historical views
    $inhibit = 1 if($station_key =~ /Unit[34]/);
    warn "Unknown station $station\n" unless($inhibit);
    return undef;
  }

  # choose station
  #$mech->click_button(id => 'ddlStation');
  $mech->field('ddlStation', $station_key);
  $mech->eval("setTimeout('__doPostBack(\\'ddlStation\\',\\'\\')', 0)");
  sleep(2);

  # choose sensor
  my $sensor_map = get_sensor_map($mech->content());

  my $sensor_key = $sensor_map->{$sensor};
  unless(defined($sensor_key)) {
    if($sensor !~ /^((spare)|(Not Found))/) {
      warn "Unknown sensor $sensor for station $station\n";
    }
    return undef;
  }

  # un-choose all stations, all sensors
  # $mech->click_button(id => 'chkAll'); # XXX As of late January 2019, the default is an unchecked box, so no need to touch. (Actually, if it's checked, it will flip the logic to all sensors except the one we want).

  $mech->click_button(id => $sensor_key);

  # reporting frequency: hourly
  $mech->field('ddlTimeBase', '60');

  $mech->click('btnGenerateReport');
  return undef unless($mech->success());
 
  if ($mech->content =~ m!<img id="C1WebChart1"!) {
    if(! -d $outdir) {
      make_path($outdir) or die "Can't create path $outdir";
    }
    $driver->capture_screenshot($outpath) or die "Can't save screenshot $outpath";
  } else {
    return undef;
  }
  return $outpath;
}

sub get_select_map {
  my ($inputname, $content) = @_;
  my %ret;
  if($content =~ m!<select \s*name\s*=\s*"${inputname}"(.*?)</select>!s) {
    my $sel = $1;
    while($sel =~ m!value="(\w+)">\s*([^<]+)</option>!g) {
      my ($key, $value) = ($1, $2);
      $ret{translate_station(encode_utf8($value))} = $key;
    }
  } else {
    warn "Bad form";
    return undef;
  }
  return \%ret;
}

sub get_sensor_map {
  my ($content) = @_;
  my %ret;
  while($content =~ m!\<input([^>]*)\>!g) {
    my $inner = $1;
    my %pairs;
    while($inner =~ m!(\w+)\s*=\s*"([^"]*)"!g) {
      $pairs{$1} = $2;
    }
    if($pairs{'type'} eq 'checkbox' && exists($pairs{'title'})) {
      my $key = encode_utf8($pairs{'title'});
      my $val = $pairs{'id'};
      if(exists($xlat_sensor{$key})) {
	$key = $xlat_sensor{$key};
      }
      $ret{$key} = $val;
    }
  }
  return \%ret;
}
