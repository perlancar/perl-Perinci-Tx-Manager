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
            note "number of actions: $num_actions";
            $num_undo_actions = @{ $res->[3]{undo_actions} };
            note "number of undo actions: $num_undo_actions";
        }


        subtest "normal action + commit" => sub {
            $tx_id = UUID::Random::generate();
            $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
            unless (is($res->[0], 200, "begin_tx succeeds")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }

            $res = $pa->request(call => $uri, {
                args => $fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
            $estatus = $targs{status} // 200;
            unless(is($res->[0], $estatus, "status is $estatus")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
            do { $done_testing++; return } unless $estatus == 200;

            $res = $pa->request(commit_tx => "/", {tx_id=>$tx_id});
            unless(is($res->[0], 200, "commit_tx succeeds")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
            $tx_id1 = $tx_id;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;
        $targs{after_do}->() if $targs{after_do};


        subtest "repeat action -> noop (idempotent), rollback" => sub {
            $tx_id = UUID::Random::generate();
            $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
            $res = $pa->request(call => $uri, {
                args => $fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
            unless(is($res->[0], 304, "status is 304")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }

            $res = $pa->request(rollback_tx => "/", {tx_id=>$tx_id});
            unless(is($res->[0], 200, "rollback_tx succeeds")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        $targs{before_undo}->() if $targs{before_undo};
        subtest "undo" => sub {
            $res = $pa->request(undo => "/", {
                tx_id=>$tx_id1, confirm=>$targs{confirm}});
            $estatus = $targs{undo_status} // 200;
            unless(is($res->[0], $estatus, "status is $estatus")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
            do { $done_testing++; return } unless $estatus == 200;
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                or note "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


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
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'R', "transaction status is R")
                        or note "res = ", explain($res);
                };

            }
            ok 1 if !$num_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "crash during rollback -> tx status X" => sub {
            $tx_id = UUID::Random::generate();

            for my $i (1..$num_actions) {
                $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
                subtest "crash at rollback #$i" => sub {
                    my $ja = 0;
                    my $jrb = 0;
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
                        $jrb++ if $args{which} eq 'rollback';
                        if ($jrb == $i) {
                            for("CRASH DURING ROLLBACK"){$log->trace($_);die $_}
                        }
                    };
                    eval {
                        $res = $pa->request(call=>$uri,
                                            {args=>$fargs,tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'X', "transaction status is X")
                        or note "res = ", explain($res);
                };
                $reset_state->();
            }
            ok 1 if !$num_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "redo" => sub {
            $res = $pa->request(redo => "/", {
                tx_id=>$tx_id1, confirm=>$targs{confirm}});
            unless (is($res->[0], 200, "redo succeeds")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                or note "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        $targs{before_undo}->() if $targs{before_undo};
        subtest "undo #2" => sub {
            $res = $pa->request(undo => "/", {
                tx_id=>$tx_id1, confirm=>$targs{confirm}});
            unless (is($res->[0], 200, "undo succeeds")) {
                note "res = ", explain($res);
                goto DONE_TESTING;
            }
            $res = $tm->list(tx_id=>$tx_id1, detail=>1);
            is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                or note "res = ", explain($res);
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "crash while undo -> roll forward" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_undo_actions) {

                # first create a committed transaction
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {
                    args=>$fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                    or note "res = ", explain($res);

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
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                        or note "res = ", explain($res);
                };

            }
            ok 1 if !$num_undo_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "crash while roll forward failed undo -> tx status X" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_undo_actions) {

                # first create a committed transaction
                $reset_state->();
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {
                    args=>$fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                    or note "res = ", explain($res);

                subtest "crash at rollback action #$i" => sub {
                    my $ju = 0; my $jrb = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        if ($args{which} eq 'undo') {
                            # first we trigger a rollback at the last step
                            if (++$ju == $num_undo_actions) {
                                for ("CRASH DURING UNDO ACTION") {
                                    $log->trace($_);die $_;
                                }
                            }
                        } elsif ($args{which} eq 'rollback') {
                            if (++$jrb == $i) {
                                for ("CRASH DURING ROLLBACK") {
                                    $log->trace($_);die $_;
                                }
                            }
                        }
                    };
                    eval {
                        $res = $pa->request(undo=>"/", {tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'X', "transaction status is X")
                        or note "res = ", explain($res);
                };

            }
            ok 1 if !$num_undo_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "crash while redo -> roll forward" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_actions) {

                $reset_state->();
                # first create an undone transaction
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {
                    args=>$fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $pa->request(undo => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                    or note "res = ", explain($res);

                subtest "crash at redo action #$i" => sub {
                    my $jrd = 0;
                    local $Perinci::Tx::Manager::_settings{default_rollback_on_action_failure} = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        my $nl = $self->{_action_nest_level} // 0;
                        return unless $args{which} eq 'redo';
                        if (++$jrd == $i) {
                            for ("CRASH DURING REDO ACTION") {
                                $log->trace($_);die $_;
                            }
                        }
                    };
                    eval {
                        $res = $pa->request(redo=>"/", {tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'C', "transaction status is C")
                        or note "res = ", explain($res);
                };

            }
            ok 1 if !$num_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


        subtest "crash while roll forward failed redo -> tx status X" => sub {
            $tx_id = UUID::Random::generate();
            for my $i (1..$num_actions) {

                # first create an undone transaction
                $reset_state->();
                $pa->request(discard_tx=>"/", {tx_id=>$tx_id});
                $pa->request(begin_tx  => "/", {tx_id=>$tx_id});
                $pa->request(call => $uri, {
                    args=>$fargs, tx_id=>$tx_id, confirm=>$targs{confirm}});
                $pa->request(commit_tx => "/", {tx_id=>$tx_id});
                $pa->request(undo => "/", {tx_id=>$tx_id});
                $res = $tm->list(tx_id=>$tx_id, detail=>1);
                is($res->[2][0]{tx_status}, 'U', "transaction status is U")
                    or note "res = ", explain($res);

                subtest "crash at rollback action #$i" => sub {
                    my $jrd = 0; my $jrb = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        if ($args{which} eq 'redo') {
                            # first we trigger a rollback at the last step
                            if (++$jrd == $num_actions) {
                                for ("CRASH DURING REDO ACTION") {
                                    $log->trace($_);die $_;
                                }
                            }
                        } elsif ($args{which} eq 'rollback') {
                            if (++$jrb == $i) {
                                for ("CRASH DURING ROLLBACK") {
                                    $log->trace($_);die $_;
                                }
                            }
                        }
                    };
                    eval {
                        $res = $pa->request(redo=>"/", {tx_id=>$tx_id});
                    };

                    # doesn't die, trapped by eval{} in _action_loop. there's
                    # also eval{} placed by periwrap
                    #ok($@, "dies") or note "res = ", explain($res);

                    # reinit TM / recover
                    $tm = Perinci::Tx::Manager->new(
                        data_dir => "$tmpdir/.tx", pa => $pa);
                    $res = $tm->list(tx_id=>$tx_id, detail=>1);
                    is($res->[2][0]{tx_status}, 'X', "transaction status is X")
                        or note "res = ", explain($res);
                };

            }
            ok 1 if !$num_actions;
        };
        goto DONE_TESTING if $done_testing || !Test::More->builder->is_passing;


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
