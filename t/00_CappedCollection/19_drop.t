#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Test::TCP";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

use bytes;
use Data::UUID;
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

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my $redis;
my $real_redis;
my $port = empty_port();

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg and !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );

# For Test::RedisServer
$real_redis->quit;
$redis = Test::RedisServer->new( conf => { port => $port }, timeout => 3 );
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $info );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

for my $big_data_threshold ( ( 0, 20 ) )
{

    $coll = Redis::CappedCollection->new(
        $redis,
        name    => "Some name",
        big_data_threshold => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );
    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    is $coll->name, "Some name", "correct collection name";

#-- all correct

# some inserts
    $len = 0;
    $tmp = 0;
    for ( my $i = 1; $i <= 10; ++$i )
    {
        ( $coll->insert( $_, $i ), $tmp += bytes::length( $_.'' ), ++$len ) for 1..10;
    }
    $info = $coll->collection_info;
    is $info->{length}, $tmp,   "OK length";
    is $info->{lists},  10,     "OK lists";
    is $info->{items},  $len,   "OK items";

    for ( my $i = 1; $i <= 10; ++$i )
    {
        $coll->drop( $i );
        $info = $coll->collection_info;
        is $info->{length}, $tmp -= 11,  "OK length";
        is $info->{lists},  10 - $i,    "OK lists";
        is $info->{items},  $len -= 10,  "OK items";
    }

    dies_ok { $coll->drop() } "expecting to die - no args";

    foreach my $arg ( ( undef, "", \"scalar", [], $uuid ) )
    {
        dies_ok { $coll->drop(
            $arg,
            ) } "expecting to die: ".( $arg || '' );
    }

    $coll->drop_collection;

}

}
