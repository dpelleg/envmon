#!/usr/bin/perl -w


harness('all', mklist('foo'), ['foo'], 'all-plain');
harness(undef, mklist('foo'), ['foo'], 'all-undef');
harness('foo', mklist('baz:foo:bar'), ['foo'], 'mask-plain');
harness('foo', mklist('fox'), [], 'mask-miss');
harness('bar,foo', mklist('baz:foo:bar'), ['foo', 'bar'], '2mask-plain');
harness('all,-foo', mklist('foo:bar'), ['bar'], 'negmask-plain');
harness('all,-foo', mklist('bar'), ['bar'], 'negmask');

sub harness {
  my ($mask, $msgs, $expected_, $testname) = @_;
  my $ret_ = test($mask, $msgs);
  my $ret = join(':', sort(map {$_->{'msg'}} @$ret_));
  my $expected = join(':', sort(@$expected_));
  print "Test: $testname\n";
  if ($ret eq $expected) {
    print "PASS\n";
  } else {
    print "FAIL : wanted $expected received $ret\n";
  }
}


sub test {
  my ($user_event_mask, $messages_) = @_;
  my @messages = @{$messages_};

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

return \@messages;
}

sub mklist {
  my ($list) = @_;
  my @list = split(/:/, $list);
  my @ret = ();
  map {
    push(@ret, { 'rule' => $_, 'msg' => $_ })
  } @list;
  return \@ret;
}

