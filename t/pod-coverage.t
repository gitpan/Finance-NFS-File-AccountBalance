#!/usr/bin/perl -w

use Test::More;
eval "use Test::Pod::Coverage 1.00";
if($@){
    plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD coverage";
} else {
    plan tests => 1;
}

my $trustme = { trustme => [qr/^trim$/] };
pod_coverage_ok( "Finance::NFS::File::AccountBalance", $trustme );

