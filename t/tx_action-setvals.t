#!perl

use 5.010;
use strict;
use warnings;
use FindBin '$Bin';
use lib "$Bin/lib";

use File::chdir;
use File::Temp qw(tempdir);
use Test::More 0.98;
use Test::Perinci::Tx::Manager qw(test_tx_action);
use TestTx;

my $tmpdir = tempdir(CLEANUP=>1);
$CWD = $tmpdir;

test_tx_action(
    tmpdir => $tmpdir,
    f      => 'TestTx::setvals',
    args   => {values=>{a=>1, b=>2, c=>3}},
);

DONE_TESTING:
done_testing();
if (Test::More->builder->is_passing) {
    #diag "all tests successful, deleting test data dir";
    $CWD = "/";
} else {
    diag "there are failing tests, not deleting test data dir $tmpdir";
}
