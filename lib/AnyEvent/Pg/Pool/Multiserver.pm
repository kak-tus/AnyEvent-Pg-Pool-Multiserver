package AnyEvent::Pg::Pool::Multiserver;

our $VERSION = '0.1';

use strict;
use warnings;
use utf8;
use v5.10;

use Carp qw( croak );
use AnyEvent;
use AnyEvent::Pg::Pool;
use Future;
use Params::Validate qw( validate_with );

use fields qw(
  pool
);

sub new {
  my $class  = shift;
  my $params = {@_};

  my $self = fields::new( $class );

  $params = $self->_validate_new( $params );

  my $pool = [];

  foreach my $server ( @{ $params->{servers} } ) {
    my $dbh = AnyEvent::Pg::Pool->new(
      $server->{conn},
      connection_retries => 10,
      connection_delay   => 1,
      on_error           => sub {},
      on_transient_error => sub {},
      on_connect_error   => sub {},
    );

    push @$pool, { dbh => $dbh, name => $server->{name}, id => $server->{id} };
  }

  $self->{pool} = $pool;

  return $self;
}

sub _validate_new {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  $params = validate_with(
    params => $params,
    spec => {
      servers => 1,
    },
  );

  return $params;
}

sub selectall_arrayref {
  my __PACKAGE__ $self = shift;
  my $params = {@_};

  $params = $self->_validate_selectall_arrayref( $params );

  my @futures = ();

  foreach my $server ( @{ $self->{pool} } ) {
    push @futures, $self->_get_future_push_query(
      query  => $params->{query},
      args   => $params->{args},
      server => $server,
    );
  }

  my $main_future = Future->wait_all( @futures );
  $main_future->on_done( sub {
    my $results = [];
    my $errors  = [];

    foreach my $future ( @futures ) {
      my ( $server, $result, $error ) = $future->get();

      if ( !$error && $result  ) {
        if ( $result->nRows ) {
          foreach my $row ( $result->rowsAsHashes ) {
            $row->{_server_id} = $server->{id};
            push @$results, $row;
          }
        }
      }
      else {
        push @$errors, {
          name => $server->{name},
          id   => $server->{id},
        };
      }
    }

    $params->{cb}->( $results, $errors );
    undef $main_future;
  } );

  return;
}

sub _validate_selectall_arrayref {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  $params = validate_with(
    params => $params,
    spec => {
      query => 1,
      args  => 0,
      cb    => 1,
    },
  );

  return $params;
}

sub _get_future_push_query {
  my __PACKAGE__ $self = shift;
  my $params = {@_};

  my $future = Future->new();

  my $watcher;

  $watcher = $params->{server}->{dbh}->push_query(
    query => $params->{query},
    args  => $params->{args},
    on_error => sub {
      $future->done( $params->{server}, undef, 1 );
      undef $watcher;
    },
    on_result => sub {
      my $p   = shift;
      my $w   = shift;
      my $res = shift;

      $future->done( $params->{server}, $res );
      undef $watcher;
    },
  );

  return $future;
}

1;

=head1 NAME

AnyEvent::Pg::Pool::Multiserver - Asyncronious multiserver requests to Postgresql with AnyEvent::Pg

=head1 SYNOPSIS

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

  # query and args same as in AnyEvent::Pg
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

=head1 DESCRIPTION

=head2 $pool->selectall_arrayref

query and args are the same, that in AnyEvent::Pg

=head1 SOURCE AVAILABILITY

The source code for this module is available from Github
at L<Github|https://github.com/kak-tus/AnyEvent-Pg-Pool-Multiserver>

=head1 AUTHOR

Andrey Kuzmin, E<lt>kak-tus@mail.ruE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by Andrey Kuzmin

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
