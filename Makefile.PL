use strict;
use warnings;
use utf8;

use ExtUtils::MakeMaker;

WriteMakefile(
  NAME           => 'AnyEvent::Pg::Pool::Multiserver',
  VERSION_FROM   => 'lib/AnyEvent/Pg/Pool/Multiserver.pm',
  PREREQ_PM      => {
  },
  ABSTRACT_FROM  => 'lib/AnyEvent/Pg/Pool/Multiserver.pm',
  AUTHOR         => 'Andrey Kuzmin <kak-tus@mail.ru>'
);
