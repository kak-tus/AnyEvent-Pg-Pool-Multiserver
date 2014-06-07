#!/usr/bin/env perl

use strict;
use warnings;
use utf8;
use v5.10;

use lib 'lib';

use AnyEvent;
use AnyEvent::Pg::Pool::Multiserver;

my $servers = [
  {
    id   => 1,
    name => 'remote 1',
    conn => 'host=remote1 port=5432 dbname=mydb user=myuser password=mypass',
  },
  {
    id   => 2,
    name => 'remote 2',
    conn => 'host=remote2 port=5432 dbname=mydb user=myuser password=mypass',
  },
];
my $pool = AnyEvent::Pg::Pool::Multiserver->new( servers => $servers );

my $cv = AE::cv;

$pool->selectall_arrayref(
  query  => 'SELECT val FROM ( SELECT 1 AS val ) tmp WHERE tmp.val = $1;',
  args   => [1],
  cb     => sub {
    my $result = shift;
    my $err    = shift;

    if ( $err ) {
      foreach my $srv ( @$err ) {
        say "err with $srv->{name} $srv->{id}";
      }
    }

    if ( $result ) {
      foreach my $val ( @$result ) {
        say "server_id=$val->{_server_id} value=$val->{val}";
      }
    }

    $cv->send;
  },
);

$cv->recv;

