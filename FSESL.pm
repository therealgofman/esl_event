package FSESL;

use strict;
use AnyEvent;
use AnyEvent::Handle;
use Data::Dumper;
use URI::Escape::XS qw(uri_escape uri_unescape);
use JSON::XS;

use constant defaults => {
	host => 'localhost',
	port => 8021,
	reconnect => 1,
	defer_connect => 0,
};

sub new {
	my ($class,%args) = @_;
	my $self = bless {
		%{&defaults},
		%args,
		connected => 0,
		wait_cmd_reply => 0,
		cq  => [], # command queue
		rq  => [], # cmd-return queue
		jw  => {}, # job watch
		pw  => {}, # any param watch
		pwv => {}, # any param watch with value
	}, $class;
	$self->_connect() unless ($self->{defer_connect});
	return $self;
}

sub _connect {
	# todo: check old conn
	my ($self,$host,$port) = @_;
	if ($self->{connected}) {
		$self->{connected} = 0;
		$self->{esl} = undef;
	}
	$self->{host} = $host if ($host);
	$self->{port} = $port if ($port);
	$self->{connected} = 1;
	$self->{esl} = new AnyEvent::Handle(
		connect => [$self->{host}, $self->{port}],
		on_error => sub {
			my ($h, $fatal, $msg) = @_;
			AE::log trace=>"* on_error *";
			$self->{connected} = 0;
			eval { $self->{on_error}->($msg) } if (ref $self->{on_error} eq 'CODE');
		},
		on_connect => sub {
			my ($h,$host,$port) = @_;
			$self->{connected} = 2;
			AE::log trace=>"* connected *";
			$self->{wait_cmd_reply} = 1;
			eval { $self->{on_connect}->($self) } if (ref $self->{on_connect} eq 'CODE');
		},
		on_read => sub {
			$self->_process_read();
		}
	);
	
}

sub _process_read {
	my ($self) = @_;
	$self->{esl}->push_read(line=>sub{
		my ($handle, $line, $eol) = @_;
		if ($line eq "") {
			my $cl = $self->{curpar}->{'content-length'};
			if (defined $cl) {
				$cl = int($cl);
				if ($cl>0) {
					$self->{esl}->push_read(chunk=>$cl, sub{
						my $cp = delete $self->{curpar};
						$cp->{'_content_'} = $_[1];
						return $self->_process_reply($cp);
					});
				} else {
					AE::log error=>"garbage in content-length!";
					# todo: disconnect
				}
			} else {
				my $cp = delete $self->{curpar};
				return $self->_process_reply($cp);
			}
		}
		if ($line =~ m/^\s*(.+?)\s*:\s*(.+?)\s*$/) {
			$self->{curpar}->{lc($1)} = uri_unescape($2);
		} elsif ($line ne '') {
			AE::log error=>"line ->$line<- unparsed";
		}
	});
}

sub _process_reply {
	my ($self, $cp) = @_;
	AE::log trace=>"*** _process_event\n";
	if ($cp->{'content-type'} eq 'command/reply') {
		$self->_process_reply_cmdreply($cp);
		$self->{wait_cmd_reply} = 0;
		$self->next_cmd();
	} elsif ($cp->{'content-type'} eq 'auth/request') {
		$self->_process_reply_authreq($cp);
	} elsif ($cp->{'content-type'} =~ m|text/event-(.+)|is) {
		if (exists $cp->{_content_}) {
			$self->_process_reply_event($cp);
		} else {
			AE::log error=>"received event without content";
		}
	# api/response
	# text/disconnect-notice
	} else {
		AE::log warn=>"no info to do with $cp->{'content-type'}";
	}
	$self->{curpar} = {};
}

sub _process_reply_cmdreply {
	my ($self, $cp) = @_;
	if (exists $cp->{'reply-text'}) {
		if ($cp->{'reply-text'} =~ m/^([+-])\s*\w+\s+(.+)/i) {
			my $ref = shift @{$self->{rq}};
			unless (defined $ref) {
				AE::log error=>"oops, command reply received, but return queue empty!";
				return;
			}
			my ($cb,$cb_err, $jobcb) = @{$ref};
			my $cb_ee = defined $cb_err ? $cb_err : $cb;
			if ($1 eq '+') {
				AE::log trace=>"good: $2";
				eval { AE::log trace=>"@ cb code"; $cb->($self, $cp) } if (ref $cb eq 'CODE');
				eval { AE::log trace=>"@ cb str"; $self->{$cb}->($self, $cp) } if (exists $self->{$cb} and ref $self->{$cb} eq 'CODE');
			} else {
				AE::log trace=>"bad: $2";
				eval { AE::log trace=>"@ cbe code"; $cb_ee->($self, $cp) } if (ref $cb_ee eq 'CODE');
				eval { AE::log trace=>"@ cbe str"; $self->{$cb_ee}->($self, $cp) } if (exists $self->{$cb_ee} and ref $self->{$cb_ee} eq 'CODE');
			}
			#
			if (exists $cp->{'job-uuid'}) {
				my $uuid = $cp->{'job-uuid'};
				if (exists $self->{jw}->{$uuid}) {
					AE::log warn=>"oops, job already exists!";
				} elsif (ref $jobcb eq 'CODE') {
					$self->{jw}->{$uuid} = $jobcb;
				}
			}
			
		} else {
			AE::log warn=>"Unknown reply prefix: $cp->{'reply-text'}";
		}
	} else {
		AE::log error=>"error: command/reply without reply-text!";
	}
}

sub _process_reply_authreq {
	my ($self, $cp) = @_;
	$self->{wait_cmd_reply} = 1;
	push @{$self->{rq}}, [ sub{
		$self->{on_auth}->($self) if (ref $self->{on_auth} eq 'CODE');
	}, sub{ 
		$self->{on_auth_error}->($self) if (ref $self->{on_auth_error} eq 'CODE');
	}];
	$self->send('auth '.$self->{pass}, 1);
}

sub _process_reply_event {
	my ($self, $cp) = @_;
	my $et = $1;
	AE::log trace=>"EVENT [$et]";
	my ($reply,%vars);
	$reply = delete $cp->{'_content_'};
	if ($et eq 'json') {
		eval { $reply = decode_json($reply); };
		return AE::log warn=>"decode_json() fail: $!" if ($!);
		for (keys %{$reply}) {
			if (m/^variable_(.+)/s) {
				$vars{$1} = delete $reply->{$_};
			}
		}
	} elsif ($et eq 'plain') {
		my (%r,$k,$v);
		for (split(/[\r]?\n/, $reply)) {
			next if ($_ eq '');
			if (($k,$v)=m/^\s*(.+?)\s*:\s*(.+)\s*$/s) {
				if ($k =~ m/^variable_(.+)/) {
					$vars{$1} = uri_unescape($v);
				} else {
					$r{$k} = uri_unescape($v);
#					$r{lc($k)} = uri_unescape($v);
				}
			} else {
				$r{ uri_unescape($_) } = 1;
			}
		}
		$reply = \%r;
	} elsif ($et eq 'xml') {
		AE::log fatal=>'event-xml not supported yet'
	} else {
		return AE::log error=>"unknown event type $et";
	}
	# lookup watchers
	my ($tr,$trv);
	foreach (keys %{$reply}) {
		next unless ($tr = $self->{pw}->{$_});
		if (exists $tr->{undef}) {
			$trv = $tr->{undef};
			delete $self->{pw}->{$_} if ($trv->{ttl} eq -1);
			AE::log trace=>"found watcher for $_ (*)";
		} elsif (exists $tr->{$reply->{$_}}) {
			$trv = $tr->{$reply->{$_}};
			delete $self->{pw}->{$_}->{$reply->{$_}} if ($trv->{ttl} eq -1);
			AE::log trace=>"found watcher for $_ ($reply->{$_})";
		} else {
			next;
		}
		eval { $trv->{cb}->($self, $reply, \%vars, $cp) };
		AE::log warn=>"on_event cb error: $!" if $!;
		return;
	}
	#
	if (exists $reply->{'Job-UUID'} and exists $self->{jw}->{ $reply->{'Job-UUID'} } ) {
		eval { $self->{jw}->{$reply->{'Job-UUID'}}->($self, $reply, \%vars, $cp); };
		AE::log warn=>"on_event job-cb error: $!" if $!;
	} else {
		return unless (exists $self->{on_event} and ref $self->{on_event} eq 'CODE');
		eval { $self->{on_event}->($self, $reply, \%vars, $cp) };
		AE::log warn=>"on_event cb error: $!" if $!;
	}
}

sub send_cmd_enq {
	my ($self, $txt, $cb, $cb_err, $cb_job) = @_;
	AE::log trace=>"*** send_cmd_enq: $txt\n";
	push @{$self->{cq}}, [ $txt, $cb, $cb_err, $cb_job ];
	$self->next_cmd();
}

sub next_cmd {
	my ($self) = @_;
	return if $self->{wait_cmd_reply};
	return unless scalar(@{$self->{cq}} );
	my $r = shift @{$self->{cq}};
	my $txt = shift @{$r};
	push @{$self->{rq}}, $r;
	$self->{wait_cmd_reply} = 1;
	$self->send($txt, 1);
}

sub send {
	my ($self, $txt, $nn) = @_;
	return undef unless ($self->{connected});
	AE::log trace=>"* send: >$txt<";
	$self->{esl}->push_write($txt.(defined $nn ? "\n\n" : ''));
}

sub watch_for {
	my ($self, $pname, $what, $cb, $ttl, $timeout) = @_;
	AE::log fatal=>"watch_for($pname, $what) without callback!" unless (ref $cb eq 'CODE');
	$timeout = 0 unless defined $timeout;
	$ttl = 86400 unless defined $ttl;
	$self->{pw}->{$pname}->{$what} = {
		pname => $pname,
		what  => $what,
		cb    => $cb,
		ttl   => $ttl,
	};
	$self->{pw}->{$pname}->{$what}->{tmr_to} = AE::timer $timeout, 0, sub {
		my $ref = delete $self->{pw}->{$pname}->{$what};
		eval { $ref->{cb}->($self,undef); };
		AE::log warn=>"watch_for() cb error: $!" if $!;
	} if $timeout>0;
	$self->{pw}->{$pname}->{$what}->{tmr_ttl} = AE::timer $ttl, 0, sub {
		delete $self->{pw}->{$pname}->{$what};
	} if $ttl>0;
}


1;