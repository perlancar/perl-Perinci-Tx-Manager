package Test::Perinci::Tx::Manager;

use 5.010;
use strict;
use warnings;
use Log::Any '$log';

use File::Remove qw(remove);
use Perinci::Access::InProcess 0.30;
use Perinci::Tx::Manager;
use Scalar::Util qw(blessed);
use Test::More 0.96;
use UUID::Random;

# VERSION

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(test_tx_action);

# note: performing transaction actions is done via riap, just for convenience as
# well as testing riap. unless when testing lower-level stuffs, where we access
# $tm and the transactional function directly.

sub test_tx_action {
    my %targs = @_;

    my $tmpdir     =$targs{tmpdir}      or die "BUG: please supply tmpdir";
    my $reset_state=$targs{reset_state} or die "BUG: please supply reset_state";

    my $tm;
    if ($targs{reset_db_dir}) {
        remove "$tmpdir/.tx";
    }

    $reset_state->();

    my $pa = Perinci::Access::InProcess->new(
        use_tx=>1,
        custom_tx_manager => sub {
            my $self = shift;
            $tm //= Perinci::Tx::Manager->new(
                data_dir => "$tmpdir/.tx", pa => $self);
            die $tm unless blessed($tm);
            $tm;
        });

    my $f = $targs{f};
    my $fargs = $targs{args} // {};
    my $tname = $targs{name} //
        "call $f => {".join(",", map{"$_=>$fargs->{$_}"} sort keys %$fargs)."}";

    subtest $tname => sub {
        my $res;
        my $estatus; # expected status
        my $tx_id;
        my ($tx_id1);
        my $done_testing;

        my $uri = "/$f"; $uri =~ s!::!/!g;

        my $num_actions = 0;
        my $num_undo_actions = 0;
        no strict 'refs';
        $res = *{$f}{CODE}->(%$fargs, -tx_action=>'check_state');
        my $has_do_actions;
        if ($res->[0] == 200) {
            if ($res->[3]{do_actions}) {
                $num_actions = @{ $res->[3]{do_actions} };
                $has_do_actions++;
            } else {
                $num_actions = 1;
            }
            diag "number of actions: $num_actions";
            $num_undo_actions = @{ $res->[3]{undo_actions} };
            diag "number of undo actions: $num_undo_actions";
        }


        subtest "normal action + commit" => sub {
            $tx_id = UUID::Random::generate();
            $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
            unless (is($res->[0], 200, "begin_tx succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }

            $res = $pa->request(call => $uri, {args => $fargs, tx_id=>$tx_id});
            $estatus = $targs{status} // 200;
            unless(is($res->[0], $estatus, "status is $estatus")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
            do { $done_testing++; return } unless $estatus == 200;

            $res = $pa->request(commit_tx => "/", {tx_id=>$tx_id});
            unless(is($res->[0], 200, "commit_tx succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
            $tx_id1 = $tx_id;
        };
        goto DONE_TESTING if $done_testing;


        subtest "repeat action -> noop (idempotent), rollback" => sub {
            $tx_id = UUID::Random::generate();
            $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
            $res = $pa->request(call => $uri, {args => $fargs, tx_id=>$tx_id});
            unless(is($res->[0], 304, "status is 304")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }

            $res = $pa->request(rollback_tx => "/", {tx_id=>$tx_id});
            unless(is($res->[0], 200, "rollback_tx succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
        };
        goto DONE_TESTING if $done_testing;


        subtest "undo" => sub {
            $res = $pa->request(undo => "/", {tx_id=>$tx_id1});
            unless (is($res->[0], 200, "undo succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                or diag "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing;


        subtest "crash during action -> rollback" => sub {
            $tx_id = UUID::Random::generate();

            for my $i (1..$num_actions) {
                $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
                subtest "crash at action #$i" => sub {
                    my $ja = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        my $nl = $self->{_action_nest_level} // 0;
                        return unless $nl <= ($has_do_actions ? 2:1);
                        return if $args{which} eq 'rollback';
                        $ja++ if $args{which} eq 'action';
                        if ($ja == $i && $nl == ($has_do_actions ? 2:1)) {
                            for ("CRASH DURING ACTION") {$log->trace($_);die $_}
                       }
                    };
                    eval {
                        $res = $pa->request(call=>$uri,
                                            {args=>$fargs,tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or diag "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'R', "transaction status is R")
                        or diag "res = ", explain($res);
                };

            }
        };
        goto DONE_TESTING if $done_testing;


        subtest "crash during rollback -> tx status X" => sub {
            $tx_id = UUID::Random::generate();

            for my $i (1..$num_actions) {
                $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
                subtest "crash at rollback #$i" => sub {
                    my $ja = 0;
                    my $jr = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        my $nl = $self->{_action_nest_level} // 0;
                        return unless $nl <= ($has_do_actions ? 2:1);
                        if ($args{which} eq 'action') {
                            # we need to trigger the rollback first, after last
                            # action
                            return unless ++$ja == $num_actions;
                            for ("CRASH DURING ACTION") {$log->trace($_);die $_}
                        }
                        $jr++ if $args{which} eq 'rollback';
                        if ($jr == $i) {
                            for("CRASH DURING ROLLBACK"){$log->trace($_);die $_}
                        }
                    };
                    eval {
                        $res = $pa->request(call=>$uri,
                                            {args=>$fargs,tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or diag "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'X', "transaction status is X")
                        or diag "res = ", explain($res);
                };
                $reset_state->();
            }
        };
        goto DONE_TESTING if $done_testing;


        subtest "redo" => sub {
            $res = $pa->request(redo => "/", {tx_id=>$tx_id1});
            unless (is($res->[0], 200, "redo succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                or diag "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing;


        subtest "undo #2" => sub {
            $res = $pa->request(undo => "/", {tx_id=>$tx_id1});
            unless (is($res->[0], 200, "undo succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                or diag "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing;


        subtest "crash while undo -> roll forward" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_undo_actions) {

                # first create a committed transaction
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {args=>$fargs, tx_id=>$tx_id});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                    or diag "res = ", explain($res);

                subtest "crash at undo action #$i" => sub {
                    my $ju = 0;
                    local $Perinci::Tx::Manager::_settings{default_rollback_on_action_failure} = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        my $nl = $self->{_action_nest_level} // 0;
                        return unless $args{which} eq 'undo';
                        if (++$ju == $i) {
                            for ("CRASH DURING UNDO ACTION") {
                                $log->trace($_);die $_;
                            }
                        }
                    };
                    eval {
                        $res = $pa->request(undo=>"/", {tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or diag "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                        or diag "res = ", explain($res);
                };

            }
        };
        goto DONE_TESTING if $done_testing;


        subtest "crash while rolling forward failed undo" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_undo_actions) {

                # first create a committed transaction
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {args=>$fargs, tx_id=>$tx_id});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                    or diag "res = ", explain($res);

                subtest "crash at undo action #$i" => sub {
                    my $ju = 0;
                    local $Perinci::Tx::Manager::_settings{default_rollback_on_action_failure} = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        my $nl = $self->{_action_nest_level} // 0;
                        return unless $args{which} eq 'undo';
                        if (++$ju == $i) {
                            for ("CRASH DURING UNDO ACTION") {
                                $log->trace($_);die $_;
                            }
                        }
                    };
                    eval {
                        $res = $pa->request(undo=>"/", {tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or diag "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                        or diag "res = ", explain($res);
                };

            }
        };
        goto DONE_TESTING if $done_testing;


        # TODO
        subtest "crash while redo -> roll forward" => sub {
            ok 1;
        };
        goto DONE_TESTING if $done_testing;


        # TODO
        subtest "crash while rolling forward failed redo" => sub {
            ok 1;
        };
        goto DONE_TESTING if $done_testing;


      DONE_TESTING:
        done_testing;
    };
}

# TODO: test cleanup: .tmp/XXX and .trash/XXX are cleaned

1;
# ABSTRACT: Transaction tests

=head1 FUNCTIONS

=head2 test_tx_action(%args)

Test performing action using transaction.

Will initialize transaction manager ($tm) and test action using $tm->call().
Will test several times with different scenarios to make sure commit, rollback,
undo, redo, and crash recoveries work.

Arguments (C<*> denotes required arguments):

=over 4

=item * tmpdir* => STR

Specify temporary directory to store transaction data directory in.

=item * name => STR

The test name.

=item * f* => STR

Fully-qualified name of transactional function, e.g. C<Setup::File::setup_file>.

=item * args* => HASH (default: {})

Arguments to feed to transactional function (via $tm->call()).

=item * reset_state* => CODE

The code to reset to initial state. This is called at the start of tests, as
well as after each rollback crash test, because crash during rollback causes the
state to become inconsistent.

=item * status => INT (default: 200)

Expect $tm->call() to return this status.

=item * reset_db_dir => BOOL (default: 0)

Whether to reset transaction data directory before running the tests. Note that
alternatively, you can also use a different C<tmpdir> for each call to this
function.

=back

=cut
