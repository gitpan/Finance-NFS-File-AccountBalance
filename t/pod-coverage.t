#!perl

use Test::More tests => 1;
eval "use Test::Pod::Coverage 1.00";
plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD coverage" if $@;

my $trustme = { trustme => [qr/^trim$/] };
pod_coverage_ok( "Finance::NFS::File::AccountBalance", $trustme );

