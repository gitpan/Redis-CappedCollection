#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";                 ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";               ## no critic
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Net::EmptyPort";                  ## no critic
    plan skip_all => "because Net::EmptyPort required for testing" if $@;
}

use bytes;
use Data::UUID;
use Time::HiRes     qw( gettimeofday );
use Redis::CappedCollection qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    ECOLLDELETED
    EREDIS
    EDATAIDEXISTS
    EOLDERTHANALLOWED
    );

use Redis::CappedCollection::Test::Utils qw(
    verify_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

$coll = Redis::CappedCollection->new(
    $redis,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

$status_key  = NAMESPACE.':status:'.$coll->name;
$queue_key   = NAMESPACE.':queue:'.$coll->name;
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

#-- all correct
@arr = $coll->pop_oldest;
is scalar( @arr ), 0, "empty array";

# some inserts
$len = 0;
$tmp = 0;
for ( my $i = 1; $i <= 10; ++$i )
{
    ( $coll->insert( $_, $i, undef, gettimeofday + 0 ), $tmp += bytes::length( $_.'' ), ++$len ) for $i..10;
}
$info = $coll->collection_info;
is $info->{length}, $tmp,   "OK length";
is $info->{lists},  10,     "OK lists";
is $info->{items},  $len,   "OK items";

for ( my $i = 1; $i <= 10; ++$i )
{
    foreach my $j ( $i..10 )
    {
        @arr = $coll->pop_oldest;
        is $arr[0], $i,    "correct id";
        is $arr[1], $j.'', "correct data";
        my $data = $arr[1];
        eval { $info = $coll->collection_info; };
        if ( $@ )
        {
            ok( ( $coll->last_errorcode == ECOLLDELETED and $i == 10 and $j == 10 ), "expecting to die" );
        }
        else
        {
            is $info->{length}, ( $tmp -= bytes::length( $data ) ), "OK length";
            is $info->{lists},  10 - $i + ( $j == 10 ? 0: 1 ),      "OK lists";
            is $info->{items},  --$len,                             "OK items";
        }
    }
}

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
