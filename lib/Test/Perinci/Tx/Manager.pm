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

    my $tmpdir = $targs{tmpdir} or die "BUG: please supply tmpdir";

    my $tm;
    if ($targs{reset_db_dir}) {
        remove "$tmpdir/.tx";
    }

    my $pa = Perinci::Access::InProcess->new(
        use_tx=>1,
        custom_tx_manager => sub {
            my $self = shift;
            $tm = Perinci::Tx::Manager->new(
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
        my $done_testing;

        my $uri = "/$f"; $uri =~ s!::!/!g;

        subtest "crash during performing action = rollback" => sub {
            my $num_actions;
            no strict 'refs';
            $res = *{$f}{CODE}->(%$fargs, -tx_action=>'check_state');
            if ($res->[3]{do_actions}) {
                $num_actions = @{ $res->[3]{do_actions} };
            } else {
                $num_actions = 1;
            }
            diag "number of actions: $num_actions";

            $tx_id = UUID::Random::generate();
            diag "tx_id = $tx_id";

            for my $i (1..$num_actions) {
                $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
                unless (is($res->[0], 200, "begin_tx succeeds")) {
                    diag "res = ", explain($res);
                    goto DONE_TESTING;
                }

                subtest "crash at action #$i" => sub {
                    my $j = 0;
                    local $Perinci::Tx::Manager::_hooks{after_fix_state} = sub {
                        my ($self, %args) = @_;
                        next if $args{which} eq 'rollback';
                        $j++ if $args{which} eq 'action';
                        if ($j == $i) {
                            $log->tracef("Crashing deliberately ...");
                            die "CRASH";
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
                    is($res->[0], 200, "list() succeeds");
                    is(scalar(@{$res->[2]}), 1, "transaction is found");
                    is($res->[2][0]{tx_status}, 'R', "transaction status is R")
                        or diag "res = ", explain($res);
                };
            }
        };
        goto DONE_TESTING if $done_testing;

        subtest "normal action + commit" => sub {
            $tx_id = UUID::Random::generate();
            diag "tx_id = $tx_id";
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
        };
        goto DONE_TESTING if $done_testing;

        subtest "repeat action = noop (idempotent), rollback" => sub {
            $tx_id = UUID::Random::generate();
            diag "tx_id = $tx_id";
            $res = $pa->request(begin_tx => "/", {tx_id=>$tx_id});
            unless (is($res->[0], 200, "begin_tx succeeds")) {
                diag "res = ", explain($res);
                goto DONE_TESTING;
            }
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

=item * status => INT (default: 200)

Expect $tm->call() to return this status.

=item * reset_db_dir => BOOL (default: 0)

Whether to reset transaction data directory before running the tests. Note that
alternatively, you can also use a different C<tmpdir> for each call to this
function.

=back

TODO:
- repeat fix state, to check idempotence
- change state after commit, to test failing undo
- change state after undo, to test failing redo

=cut

__END__
        subtest "before setup" => sub {
            $chku->();
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "do (dry run)" => sub {
            my %fargs = (%$fargs,  -undo_action=>'do', -tx_manager=>$tm,
                         -dry_run=>1);
            $res = $func->(%fargs);
            if ($targs{dry_do_error}) {
                is($res->[0], $targs{dry_do_error},
                   "status is $targs{dry_do_error}");
                $exit++;
            } else {
                if (is($res->[0], 200, "status 200")) {
                    $chku->();
                    $undo_data = $res->[3]{undo_data};
                    ok($undo_data, "function returns undo_data");
                } else {
                    diag "res = ", explain($res);
                };
            }
            done_testing;
        };
        goto END_TESTS if $exit;
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "do" => sub {
            my %fargs = (%$fargs,  -undo_action=>'do', -tx_manager=>$tm);
            $res = $func->(%fargs);
            if ($targs{do_error}) {
                is($res->[0], $targs{do_error},
                   "status is $targs{do_error}");
                $exit++;
            } else {
                if (is($res->[0], 200, "status 200")) {
                    $chks->();
                    $undo_data = $res->[3]{undo_data};
                    ok($undo_data, "function returns undo_data");
                } else {
                    diag "res = ", explain($res);
                }
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "repeat do -> noop (idempotent)" => sub {
            my %fargs = (%$fargs,  -undo_action=>'do', -tx_manager=>$tm);
            $res = $func->(%fargs);
            if (is($res->[0], 304, "status 304")) {
                $chks->();
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        if ($targs{set_state1} && $targs{check_state1}) {
            $targs{set_state1}->();
            subtest "undo after state changed" => sub {
                my %fargs = (%$fargs, -undo_action=>'undo', -tx_manager=>$tm,
                             -undo_data=>$undo_data);
                $res = $func->(%fargs);
                $targs{check_state1}->();
                done_testing;
            };
            goto END_TESTS;
        }

        subtest "undo (dry run)" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo', -undo_data=>$undo_data,
                         -tx_manager=>$tm, -dry_run=>1);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chks->();
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "undo" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo', -undo_data=>$undo_data,
                         -tx_manager=>$tm);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chku->();
                $redo_data = $res->[3]{undo_data};
                ok($redo_data, "function returns undo_data (for redo)");
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        # note: repeat undo is NOT guaranteed to be noop, not idempotent here
        # because we rely on undo data which will refuse to apply changes if
        # state has changed.

        if ($targs{set_state2} && $targs{check_state2}) {
            $targs{set_state2}->();
            subtest "redo after state changed" => sub {
                my %fargs = (%$fargs, -undo_action=>'undo',
                             -undo_data=>$redo_data, -tx_manager=>$tm);
                $res = $func->(%fargs);
                $targs{check_state2}->();
                done_testing;
            };
            goto END_TESTS;
        }

        subtest "redo (dry run)" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo', -undo_data=>$redo_data,
                         -tx_manager=>$tm, -dry_run=>1);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chku->();
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "redo" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo', -undo_data=>$redo_data,
                         -tx_manager=>$tm);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chks->();
                $undo_data2 = $res->[3]{undo_data};
                ok($undo_data2,"function returns undo_data (for undoing redo)");
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        # note: repeat redo is NOT guaranteed to be noop.

        subtest "undo redo (dry run)" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo', -undo_data=>$undo_data2,
                         -tx_manager=>$tm, -dry_run=>1);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chks->();
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        subtest "undo redo" => sub {
            my %fargs = (%$fargs, -undo_action=>'undo',
                         -undo_data=>$undo_data2, -tx_manager=>$tm);
            $res = $func->(%fargs);
            if (is($res->[0], 200, "status 200")) {
                $chku->();
                #$redo_data2 = $res->[3]{undo_data};
                #ok($redo_data2, "function returns undo_data");
            } else {
                diag "res = ", explain($res);
            }
            done_testing;
        };
        goto END_TESTS unless Test::More->builder->is_passing;

        # note: repeat undo redo is NOT guaranteed to be noop.

        ## can no longer be done, perigen-undo 0.25+ requires tx
        #subtest "normal (without undo) (dry run)" => sub {
        #    my %fargs = (%$fargs,
        #                 -dry_run=>1);
        #    $res = $func->(%fargs);
        #    $chku->();
        #    done_testing;
        #};
        #goto END_TESTS unless Test::More->builder->is_passing;
        #
        #subtest "normal (without undo)" => sub {
        #    my %fargs = (%$fargs);
        #    $res = $func->(%fargs);
        #    $chks->();
        #    done_testing;
        #};
        #goto END_TESTS unless Test::More->builder->is_passing;
        #
        #subtest "repeat normal -> noop (idempotent)" => sub {
        #    my %fargs = (%$fargs);
        #    $res = $func->(%fargs);
        #    $chks->();
        #    is($res->[0], 304, "status 304");
        #    done_testing;
        #};
        #goto END_TESTS unless Test::More->builder->is_passing;

