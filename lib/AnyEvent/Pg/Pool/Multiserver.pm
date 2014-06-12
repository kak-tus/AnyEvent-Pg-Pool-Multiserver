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

  my $pool = {};

  foreach my $server ( @{ $params->{servers} } ) {
    my $dbh = AnyEvent::Pg::Pool->new(
      $server->{conn},
      connection_retries => 10,
      connection_delay   => 1,
      size               => 4,
      on_error           => sub {},
      on_transient_error => sub {},
      on_connect_error   => sub {},
    );

    croak 'server_id must be unique' if $pool->{ $server->{id} };
    $pool->{ $server->{id} } = { dbh => $dbh, name => $server->{name}, id => $server->{id} };
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

  my @pool = ();
  if ( defined $params->{server_id} ) {
    push @pool, $params->{server_id};
  }
  else {
    @pool = keys %{ $self->{pool} };
  }

  foreach my $server_id ( @pool ) {
    my $server = $self->{pool}{$server_id};
    push @futures, $self->_get_future_push_query(
      query     => $params->{query},
      args      => $params->{args},
      server    => $server,
      cb_server => $params->{cb_server},
      type      => 'selectall_arrayref_slice',
    );
  }

  my $main_future = Future->wait_all( @futures );
  $main_future->on_done( sub {
    my $results = [];
    my $errors  = [];

    foreach my $future ( @futures ) {
      my ( $server, $result, $error ) = $future->get();

      if ( !$error ) {
        push @$results, @$result;
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
      query     => 1,
      args      => 0,
      cb        => 1,
      server_id => 0,
      cb_server => 0,
    },
  );

  return $params;
}

sub _get_future_push_query {
  my __PACKAGE__ $self = shift;
  my $params = {@_};

  my $future = Future->new();

  my $watcher;

  $watcher = $params->{server}{dbh}->push_query(
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

      my $result;

      if ( $params->{type} eq 'selectall_arrayref_slice' ) {
        $result = _fetchall_arrayref_slice( $params->{server}{id}, $res );
      }
      elsif ( $params->{type} eq 'selectrow_hashref' ) {
        $result = _fetchrow_hashref( $params->{server}{id}, $res );
      }

      my $cb = sub {
        $future->done( $params->{server}, $result );
        undef $watcher;
      };

      if ( $params->{cb_server} ) {
        $params->{cb_server}->( result => $result, cb => $cb );
      }
      else {
        $cb->();
      }
    },
  );

  return $future;
}

sub _fetchall_arrayref_slice {
  my $id  = shift;
  my $res = shift;

  my $result = [];

  if ( $res->nRows ) {
    foreach my $row ( $res->rowsAsHashes ) {
      $row->{_server_id} = $id;
      push @$result, $row;
    }
  }

  return $result;
}

sub _fetchrow_hashref {
  my $id  = shift;
  my $res = shift;

  my $result;

  if ( $res->nRows ) {
    $result = $res->rowAsHash(0);
    $result->{_server_id} = $id;
  }

  return $result;
}

sub selectrow_hashref {
  my __PACKAGE__ $self = shift;
  my $params = {@_};

  $params = $self->_validate_selectrow_hashref( $params );

  my $future = $self->_get_future_push_query(
    query     => $params->{query},
    args      => $params->{args},
    server    => $self->{pool}{ $params->{server_id} },
    cb_server => $params->{cb_server},
    type      => 'selectrow_hashref',
  );

  $future->on_done( sub {
    my ( $server, $result, $error ) = $future->get();

    if ( !$error ) {
      $params->{cb}->( $result );
    }
    else {
      $params->{cb}->( undef, {
        name => $server->{name},
        id   => $server->{id},
      } );
    }

    undef $future;
  } );

  return;
}

sub _validate_selectrow_hashref {
  my __PACKAGE__ $self = shift;
  my $params = shift;

  $params = validate_with(
    params => $params,
    spec => {
      query     => 1,
      args      => 0,
      cb        => 1,
      server_id => 1,
      cb_server => 0,
    },
  );

  return $params;
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
