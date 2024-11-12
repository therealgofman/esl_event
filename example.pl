#!/usr/bin/perl

use strict;
use FSESL;
use AnyEvent;
use AnyEvent::Log;
use Data::UUID;

#condvar
my $cv = AE::cv;

sub generate_uuid {
	my $ug = Data::UUID->new;
	my $uuid = $ug->create();
	my $str_uuid = $ug->to_string( $uuid );
	return $str_uuid;
}

my $fs = FSESL->new(
	'pass' => 'ClueCon',
	on_auth => sub {
			AE::log info => 'AUTH OK'; 
			my ($self) = @;
			$self->send_cmd_enq('event json ALL');

			my $uuid = generate_uuid();
			my $cmd = "bgapi originate "; 
			$cmd .= "{origination_uuid=$uuid,originate_timeout=30,dolg=300,";
			$cmd .= "ignore_early_media=true,origination_caller_id_number=3433820382,lic=15123,";
			$cmd .= "origination_caller_id_name=3433820382}sofia/gateway/Mera/2861212 obzvon";
			$self->send_cmd_enq($cmd, sub {
				my ( $self, $ev ) = @; 
				#do something
			}); 
			#Ожидаем событие
			$self->watch_for('Event-Name', 'CHANNEL_DESTROY', sub {
				my ( $self, $ev, $ev2 ) = @;
				#do something
			},0,0); 
	},
	on_auth_error => sub {
		AE::log info => 'Auth Fail!';
		$cv->send;
	},
	on_connect => sub {
		AE::log info => 'Connect Socket FS - OK!';
	},
	on_error => sub {
		AE::log error => "FSESL error: $[0]"; $cv->send;
	});

#loop
$cv->recv;