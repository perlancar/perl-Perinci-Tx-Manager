package Perinci::Tx::Manager;

use 5.010;
use strict;
use warnings;
use DBI;
use File::Flock;
use JSON;
use Log::Any '$log';
use Scalar::Util qw(blessed);
use Time::HiRes qw(time);

# VERSION

my $json = JSON->new->allow_nonref;

# note: to avoid confusion, whenever we mention 'transaction' (or tx for short)
# in the code, we must always specify whether it is a sqlite tx (sqltx) or a
# Rinci tx (Rtx).

# note: no method should die(), they all should return error message/response
# instead. this is because we are called by Perinci::Access::InProcess and in
# turn it is called by Perinci::Access::HTTP::Server without extra eval(). an
# exception is in _init() when we don't want to deal with old data.

# note: we have not dealt with sqlite's rowid wraparound. since it's a 64-bit
# integer, we're pretty safe. we also usually rely on ctime first for sorting.

# new() should return an error string if failed
sub new {
    my ($class, %opts) = @_;
    return "Please supply pa object" unless blessed $opts{pa};
    return "pa object must be an instance of Perinci::Access::InProcess"
        unless $opts{pa}->isa("Perinci::Access::InProcess");

    my $obj = bless \%opts, $class;
    if ($opts{data_dir}) {
        unless (-d $opts{data_dir}) {
            mkdir $opts{data_dir} or return "Can't mkdir $opts{data_dir}: $!";
        }
    } else {
        for ("$ENV{HOME}/.perinci", "$ENV{HOME}/.perinci/.tx") {
            unless (-d $_) {
                mkdir $_ or return "Can't mkdir $_: $!";
            }
        }
        $opts{data_dir} = "$ENV{HOME}/.perinci/.tx";
    }
    my $res = $obj->_init;
    return $res if $res;
    $obj;
}

sub _lock_db {
    my ($self, $shared) = @_;

    my $locked;
    my $secs = 0;
    for (1..5) {
        # we don't lock the db file itself because on some OS's like OpenBSD,
        # this results in 'DB is locked' SQLite error.
        $locked = lock("$self->{_db_file}.lck", $shared, "nonblocking");
        last if $locked;
        sleep    $_;
        $secs += $_;
    }
    return "Tx database is still locked by other process (probably recovery) ".
        "after $secs seconds, giving up" unless $locked;
    return;
}

sub _unlock_db {
    my ($self) = @_;

    unlock("$self->{_db_file}.lck");
    return;
}

# return undef on success, or an error string on failure
sub _init {
    my ($self) = @_;
    my $data_dir = $self->{data_dir};
    $log->tracef("[tm] Initializing data dir %s ...", $data_dir);

    unless (-d "$self->{data_dir}/.trash") {
        mkdir "$self->{data_dir}/.trash"
            or return "Can't create .trash dir: $!";
    }
    unless (-d "$self->{data_dir}/.tmp") {
        mkdir "$self->{data_dir}/.tmp"
            or return "Can't create .tmp dir: $!";
    }

    $self->{_db_file} = "$data_dir/tx.db";

    (-d $data_dir)
        or return "Transaction data dir ($data_dir) doesn't exist or not a dir";
    my $dbh = DBI->connect("dbi:SQLite:dbname=$self->{_db_file}", undef, undef,
                           {RaiseError=>0});

    # init database

    my $ep = "Can't init tx db:"; # error prefix

    $dbh->do(<<_) or return "$ep create tx: ". $dbh->errstr;
CREATE TABLE IF NOT EXISTS tx (
    ser_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    str_id VARCHAR(200) NOT NULL,
    owner_id VARCHAR(64) NOT NULL,
    summary TEXT,
    status CHAR(1) NOT NULL, -- i, a, C, U, R, u, v, d, e, X [uppercase=final]
    ctime REAL NOT NULL,
    commit_time REAL,
    last_call_id INTEGER, -- last processed call (or undo_call) when rollback
    UNIQUE (str_id)
)
_

    # last_call_id is for the recovery process to avoid repeating all the
    # function calls when rollback/undo/redo failed in the middle. for example,
    # tx1 (status=i) has 3 calls: c1, c2, c3. tx1 is being rollbacked
    # (status=a). tm executes c3, then c2, then crashes before calling c1. since
    # last_call_id is set to c2 at the end of calling c2, then during recovery,
    # rollback continues at c1.

    $dbh->do(<<_) or return "$ep create call: ". $dbh->errstr;
CREATE TABLE IF NOT EXISTS call (
    tx_ser_id INTEGER NOT NULL, -- refers tx(ser_id)
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    sp TEXT NOT NULL, -- for named savepoint
    ctime REAL NOT NULL,
    f TEXT NOT NULL,
    args TEXT NOT NULL,
    UNIQUE(sp)
)
_

    $dbh->do(<<_) or return "$ep create undo_call: ". $dbh->errstr;
CREATE TABLE IF NOT EXISTS undo_call (
    tx_ser_id INTEGER NOT NULL, -- refers tx(ser_id)
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    sp TEXT NOT NULL, -- for named savepoint
    ctime REAL NOT NULL,
    f TEXT NOT NULL,
    args TEXT NOT NULL,
    UNIQUE(sp)
)
_

    $dbh->do(<<_) or return "$ep create _meta: ".$dbh->errstr;
CREATE TABLE IF NOT EXISTS _meta (
    name TEXT PRIMARY KEY NOT NULL,
    value TEXT
)
_
    $dbh->do(<<_) or return "$ep insert v: ".$dbh->errstr;
-- v is incremented everytime schema changes
INSERT OR IGNORE INTO _meta VALUES ('v', '4')
_

    # deal with table structure changes
  UPDATE_SCHEMA:
    while (1) {
        my ($v) = $dbh->selectrow_array(
            "SELECT value FROM _meta WHERE name='v'");
        if ($v <= 3) {

            # changes incompatible (no longer undo_step and redo_step tables),
            # can lose data. we bail and let user decide for herself.

            die join(
                "",
                "Your transaction database ($self->{_db_file}) is still at v=3",
                ", there is incompatible changes with newer version. ",
                "Either delete the transaction database (and lose undo data) ",
                "or use an older version of ".__PACKAGE__." (0.28 or older).\n",
            );

        #} elsif ($v == x) {
        #
        #    $dbh->begin_work;
        #
        #    # ...
        #
        #    $dbh->commit;

        } else {
            # already the latest schema version
            last UPDATE_SCHEMA;
        }
    }

    $self->{_dbh} = $dbh;
    $log->tracef("[tm] Data dir initialization finished");
    $self->_recover;
}

sub get_trash_dir {
    my ($self) = @_;
    my $tx = $self->{_cur_tx};
    return [412, "No current transaction, won't create trash dir"] unless $tx;
    my $d = "$self->{data_dir}/.trash/$tx->{ser_id}";
    unless (-d $d) {
        mkdir $d or return [500, "Can't mkdir $d: $!"];
    }
    [200, "OK", $d];
}

sub get_tmp_dir {
    my ($self) = @_;
    my $tx = $self->{_cur_tx};
    return [412, "No current transaction, won't create tmp dir"] unless $tx;
    my $d = "$self->{data_dir}/.tmp/$tx->{ser_id}";
    unless (-d $d) {
        mkdir $d or return [500, "Can't mkdir $d: $!"];
    }
    [200, "OK", $d];
}

# return an enveloped response
sub _get_func_and_meta {
    my ($self, $func) = @_;

    my ($module, $leaf) = $func =~ /(.+)::(.+)/
        or return [400, "Not a valid fully qualified function name: $func"];
    my $module_p = $module; $module_p =~ s!::!/!g; $module_p .= ".pm";
    eval { require $module_p }
        or return [500, "Can't load module $module: $@"];
    # get metadata as well as wrapped
    my $res = $self->{pa}->_get_code_and_meta({
        -module=>$module, -leaf=>$leaf, -type=>'function'});
    $res;
}

sub _rollback_dbh {
    my $self = shift;
    $self->{_dbh}->rollback if $self->{_in_sqltx};
    $self->{_in_sqltx} = 0;
}

sub _commit_dbh {
    my $self = shift;
    return 1 unless $self->{_in_sqltx};
    my $res = $self->{_dbh}->commit;
    $self->{_in_sqltx} = 0;
    $res;
}

sub __test_tx_feature {
    my $meta = shift;
    my $ff = $meta->{features} // {};
    $ff->{tx} && ($ff->{tx}{use} || $ff->{tx}{req}) &&
        $ff->{undo} && $ff->{dry_run};
}

# check calls (or undo data), whether function and metadata exists, whether
# function supports transaction. return undef un success, or an error string on
# failure. cache func in call[4].
sub _check_calls {
    my ($self, $calls, $decode) = @_;
    return "not an array" unless ref($calls) eq 'ARRAY';
    my $i = 0;
    for my $c (@$calls) {
        my $ep = "call[$i] (function $c->[0])";
        return "$ep: not an array" unless ref($c) eq 'ARRAY';
        eval {
            $c->[1] = $json->decode($c->[1]) if $c->[1] && $decode;
            $c->[2] = $json->decode($c->[2]) if $c->[2] && $decode;
        };
        return "$ep: can't deserialize data: $@" if $@;
        my $res = $self->_get_func_and_meta($c->[0]);
        return "$ep: can't get metadata" unless $res->[0] == 200;
        my ($func, $meta) = @{$res->[2]};
        return "$ep: function does not support transaction"
            unless __test_tx_feature($meta);
        $c->[4] = $func;
        $i++;
    }
    return;
}

# rollback, undo, redo, call share a fair amount of code, mainly looping
# through function calls, so we combine them here.
#
# return undef on success, or an error string on failure.
sub _loop_calls {
    # $calls is only for which='call', for rollback/undo/redo, the list of calls
    # is taken from the database table.
    my ($self, $which, $calls, $opts) = @_;
    $opts //= {};

    # log prefix
    my $lp = "[tm] [$which]";

    return "BUG: 'which' must be rollback/undo/redo/call"
        unless $which =~ /\A(rollback|undo|redo|call)\z/;
    my $rb = $which eq 'rollback';

    # this prevent endless loop in rollback, since we call functions when doing
    # rollback, and functions might call $tx->rollback too upon failure.
    return if $self->{_in_rollback} && $rb;
    local $self->{_in_rollback} = 1 if $rb;

    my $tx = $self->{_cur_tx};
    return "called w/o transaction, probably a bug" unless $tx;

    $log->tracef("$lp tx #%d (%s) ...", $tx->{ser_id}, $tx->{str_id});

    my $dbh = $self->{_dbh};
    $self->_rollback_dbh;
    # we're now in sqlite autocommit mode, we use this mode for the following
    # reasons: 1) after we set Rtx status to a/e/v/u/d, we need other clients to
    # immediately see this, so e.g. if Rtx was i, they do not try to add steps
    # to it. also after that, each function call will involve one or several
    # _record_call(), each of which is a separate sqltx so each call can be
    # recorded permanently in sqldb.

    # first we need to set the appropriate transaction status first, to prevent
    # other clients from interfering/racing.
    my $os = $tx->{status};
    my $ns = $os;
    my $fs =
        ($os eq 'i' || $os eq 'a') ? 'R' :
            $os eq 'u' ? 'U' :
                $os eq 'v' ? 'C' :
                    $os eq 'd' ? 'C' :
                        'U';
    if ($rb) {
        $ns = $os eq 'i' ? 'a' : $os eq 'u' ? 'v' : $os eq 'd' ? 'e' : $os;
        if ($ns ne $os) {
            $dbh->do("UPDATE tx SET status='$ns', last_call_id=NULL ".
                         "WHERE ser_id=?", {}, $tx->{ser_id})
                or return "db: Can't update tx status $os -> $ns: ".
                    $dbh->errstr;
            # to make sure, check once again if Rtx status is indeed updated
            my @r = $dbh->selectrow_array(
                "SELECT status FROM tx WHERE ser_id=?", {}, $tx->{ser_id});
            return "Can't update tx status #2 (wants $ns, still $r[0])"
                unless $r[0] eq $ns;
        }
    }

    # for the main processing, we setup a giant eval loop. any error during
    # processing, we return() from the eval and trigger a rollback (unless we
    # are the rollback process itself, in which case we set tx status to X and
    # give up).

    my $eval_err = eval {
        my $res;

        # gt=table to get our calls from, ut=table to record undo calls to
        my ($gt, $ut);
        my $reverse; # whether we should reverse the order list of calls from db
        if ($which eq 'call') {
            $gt = undef;
            $ut = 'undo_call';
        } elsif ($which eq 'undo') {
            $gt = 'undo_call';
            $reverse = 1;
            $ut = 'call';
        } elsif ($which eq 'redo') {
            $gt = 'call';
            $reverse = 0;
            $ut = 'undo_call';
        } elsif ($which eq 'rollback') {
            $gt = $ns eq 'v' ? "call" : "undo_call";
            $reverse = $ns eq 'v' ? 0 : 1;
            $ut = undef;
        }
        if ($gt) {
            # get the list of calls from database table: [[0]f, [1]args, [2]cid,
            # [3]\&code]
            my $lci = $tx->{last_call_id};
            $calls = $dbh->selectall_arrayref(join(
                "",
                "SELECT f, args, id FROM $gt WHERE tx_ser_id=? ",
                ($lci ? "AND (id<>$lci AND ".
                     "ctime ".($reverse ? "<=" : ">=").
                         " (SELECT ctime FROM $gt WHERE id=$lci))":""),
                "ORDER BY ctime, id"), {}, $tx->{ser_id});
            $calls = [reverse @$calls] if $reverse;
            $log->tracef("$lp Calls to perform: %s", $calls);
        }

        # check the calls
        $res = $self->_check_calls($calls, $gt ? 'decode_json':'');
        return "invalid calls data: $res" if $res;

        my $i = 0;
        my $sp_recorded;
        for my $c (@$calls) {
            my $lp = "$lp [call[$i] (function $c->[0])]";
            my $ep = "call[$i] (function $c->[0])";
            my %args = %{$c->[1] // {}};
            for (keys %args) { delete $args{$_} if /^-/ } # strip special args
            $args{-tx_manager}  = $self;
            # the following special arg is just informative, so function knows
            # and can act more robust under rollback if it needs to
            $args{-tx_action}   = 'rollback' if $rb;
            $args{-undo_action} = 'do';
            if ($ut) {
                # call function with -dry_run=>1 first, to get undo data
                $args{-dry_run} = 1;
                $args{-log_call} = 0;
                $res = $c->[4]->(%args);
                return "$ep: can't get undo data: $res->[0] - $res->[1]"
                    unless $res->[0] == 200 || $res->[0] == 304;
                my $undo_data = $res->[3]{undo_data};
                my $ures = $self->_check_calls($undo_data);
                return "$ep: invalid undo data: $ures" if $ures;
                # record undo data (undo calls)
                my $j = 0;
                for my $uc (@$undo_data) {
                    my $ep = "$ep undo_data[$j] ($uc->[0])";
                    my $ctime = time();
                    # XXX make sure ctime is incremented for every item, because
                    # otherwise there's a very slight chance that ID wraparound
                    # + identical time = out of order. quite slim though
                    eval { $uc->[1] = $json->encode($uc->[1]) };
                    return "$ep: can't serialize: $@" if $@;
                    # insert savepoint name for the first undo_call only
                    my $sp = $sp_recorded++ ? undef : $opts->{sp};
                    $dbh->do(
                        "INSERT INTO $ut (tx_ser_id, sp, ctime, f, args) ".
                            "VALUES (?,?,?,?,?)", {},
                        $tx->{ser_id}, $sp, $ctime, $uc->[0], $uc->[1])
                        or return "$ep: db: can't insert $ut: ".$dbh->errstr;
                    $j++;
                }
            }

            # call function "for real" this time
            $log->tracef("$lp %d/%d Call (%s), undo_data: %s ...",
                         $i, scalar(@$calls), $c->[1], $c->[2]);
            $res = $c->[4]->(%args); # we have previously save func to $c->[4]
            $log->tracef("$lp Call result: %s", $res);
            return "$ep: call failed: $res->[0] - $res->[1]"
                unless $res->[0] == 200 || $res->[0] == 304;

            # store temporarily to object, since we need to return undef on
            # success.
            $self->{_res} = $res;

            # update last_call_id so we don't have to repeat all steps after
            # recovery. error can be ignored here, i think.
            if ($c->[3]) {
                $dbh->do("UPDATE tx SET last_call_id=? WHERE ser_id=?", {},
                         $c->[3]);
            }
        } # for call

        # if we are have filled up undo_call, empty call, and vice versa.
        if ($ut) {
            my $t = $ut eq 'call' ? 'undo_call' : 'call';
            $dbh->do("DELETE FROM $t WHERE id IN ".
                         "(SELECT id FROM call WHERE tx_ser_id=?)",
                     {}, $tx->{ser_id})
                or return "db: Can't empty $t: ".$dbh->errstr;
        }

        # set transaction final status
        $dbh->do("UPDATE tx SET status='$fs' WHERE ser_id=?", {}, $tx->{ser_id})
            or return "db: Can't set tx status to $fs: ".$dbh->errstr;

        return;
    }; # eval

    if ($eval_err) {
        if ($rb) {
            # if failed during rolling back, we don't know what else to do. we
            # set Rtx status to X (inconsistent) and ignore it.
            $dbh->do("UPDATE tx SET status='X' WHERE ser_id=?",
                     {}, $tx->{ser_id});
            return $eval_err;
        } else {
            my $rbres = $self->_rollback;
            if ($rbres) {
                return $eval_err.
                    " (rollback failed: $rbres->[0] - $rbres->[1])";
            } else {
                return $eval_err." (rolled back)";
            }
        }
    }
    return;
}

# return undef on success, or an error string on failure
sub _recover_or_cleanup {
    my ($self, $which) = @_;

    # TODO clean old tx's tmp_dir & trash_dir.

    $log->tracef("[tm] Performing $which ...");

    # there should be only one process running
    my $res = $self->_lock_db(undef);
    return $res if $res;

    # rolls back all transactions in a, u, d state

    # XXX when cleanup, also rolls back all i transactions that have been around
    # for too long
    my $dbh = $self->{_dbh};
    my $sth = $dbh->prepare(
        "SELECT * FROM tx WHERE status IN ('a', 'u', 'd') ".
            "ORDER BY ctime DESC",
    );
    $sth->execute or return "db: Can't select tx: ".$dbh->errstr;

    while (my $row = $sth->fetchrow_hashref) {
        $self->{_cur_tx} = $row;
        $self->_rollback;
    }

    $self->_unlock_db;

    # XXX when cleanup, discard all R Rtxs

    # XXX when cleanup, discard all C, U, X Rtxs that have been around too long

    $log->tracef("[tm] Finished $which");
    return;
}

sub _recover {
    my $self = shift;
    $self->_recover_or_cleanup('recover');
}

sub _cleanup {
    my $self = shift;
    $self->_recover_or_cleanup('cleanup');
}

sub __resp_tx_status {
    state $statuses = {
        i => 'still in-progress',
        a => 'aborted (further requests ignored until rolled back)',
        v => 'aborted undo (further requests ignored until rolled back)',
        e => 'aborted redo (further requests ignored until rolled back)',
        C => 'already committed',
        R => 'already rolled back',
        U => 'already committed+undone',
        u => 'undoing',
        d => 'redoing',
        X => 'inconsistent',
    };
    my ($r) = @_;
    my $s   = $r->{status};
    my $ss  = $statuses->{$s} // "unknown (bug)";
    [480, "tx #$r->{ser_id}: Incorrect status, status is $s ($ss)"];
}

# all methods that work inside a transaction have some common code, e.g.
# database file locking, starting sqltx, checking Rtx status, etc. hence
# refactored into _wrap(). arguments:
#
# - label (string, just a label for logging)
#
# - args* (hashref, arguments to method)
#
# - cleanup (bool, default 0). whether to run cleanup first before code. this is
#   curently run by begin() only, to make up room by purging old transactions.
#
# - tx_status (str/array, if set then it means method requires Rtx to exist and
#   have a certain status(es)
#
# - code (coderef, main method code, will be passed args as hash)
#
# - hook_check_args (coderef, will be passed args as hash)
#
# - hook_after_commit (coderef, will be passed args as hash).
#
# wrap() will also put current Rtx record to $self->{_cur_tx}
sub _wrap {
    my ($self, %wargs) = @_;
    my $margs = $wargs{args}
        or return [500, "BUG: args not passed to _wrap()"];
    my @caller = caller(1);
    $log->tracef(
        "[tm] -> %s(%s) label=%s",
        $caller[3],
        { map {$_=>$margs->{$_}} grep {!/^-/ && !/^args$/} keys %$margs },
        $wargs{label},
    );

    my $res;

    $res = $self->_lock_db("shared");
    return [532, "Can't acquire lock: $res"] if $res;

    $self->{_now} = time();

    # initialize & check tx_id argument
    $margs->{tx_id} //= $self->{_tx_id};
    my $tx_id = $margs->{tx_id};
    $self->{_tx_id} = $tx_id;

    return [400, "Please specify tx_id"]
        unless defined($tx_id) && length($tx_id);
    return [400, "Invalid tx_id, please use 1-200 characters only"]
        unless length($tx_id) <= 200;

    my $dbh = $self->{_dbh};

    if ($wargs{cleanup}) {
        $res = $self->_cleanup;
        return [532, "Can't succesfully cleanup: $res"] if $res;
    }

    # we need to begin sqltx here so that client's actions like rollback() and
    # commit() are indeed atomic and do not interfere with other clients'.
    $dbh->begin_work or return [532, "db: Can't begin: ".$dbh->errstr];

    # DBI/DBD::SQLite currently does not support checking whether we are in an
    # active sqltx, except $dbh->{BegunWork} which is undocumented. we use our
    # own flag here.
    local $self->{_in_sqltx} = 1;

    my $cur_tx = $dbh->selectrow_hashref(
        "SELECT * FROM tx WHERE str_id=?", {}, $tx_id);
    $self->{_cur_tx} = $cur_tx;

    if ($wargs{hook_check_args}) {
        $res = $wargs{hook_check_args}->(%$margs);
        if ($res) {
            $self->_rollback;
            return $res;
        }
    }

    if ($wargs{tx_status}) {
        if (!$cur_tx) {
            $self->_rollback_dbh;
            return [484, "No such transaction"];
        }
        my $ok;
        # 'str' ~~ $aryref doesn't seem to work?
        if (ref($wargs{tx_status}) eq 'ARRAY') {
            $ok = $cur_tx->{status} ~~ @{$wargs{tx_status}};
        } else {
            $ok = $cur_tx->{status} ~~ $wargs{tx_status};
        }
        unless ($ok) {
            $self->_rollback_dbh;
            return __resp_tx_status($cur_tx);
        }
    }

    if ($wargs{code}) {
        $res = $wargs{code}->(%$margs, _tx=>$cur_tx);
        $log->tracef("code res: %s", $res);
        # on error, rollback and skip the rest
        if ($res->[0] >= 400) {
            $self->_rollback if $res->[3]{rollback} // 1;
            return $res;
        }
    }

    $self->_commit_dbh or return [532, "db: Can't commit: ".$dbh->errstr];
    $self->{_in_sqltx} = 0;

    if ($wargs{hook_after_commit}) {
        my $res2 = $wargs{hook_after_tx}->(%$margs);
        return $res2 if $res2;
    }

    return $res;
}

# all methods that don't work inside a transaction have some common code, e.g.
# database file locking. arguments:
#
# - args* (hashref, arguments to method)
#
# - lock_db (bool, default false)
#
# - code* (coderef, main method code, will be passed args as hash)
#
sub _wrap2 {
    my ($self, %wargs) = @_;
    my $margs = $wargs{args}
        or return [500, "BUG: args not passed to _wrap()"];
    my @caller = caller(1);
    $log->tracef("[tm] -> %s(%s)", $caller[3],
                 { map {$_=>$margs->{$_}}
                       grep {!/^-/ && !/^args$/} keys %$margs });

    my $res;

    if ($wargs{lock_db}) {
        $res = $self->_lock_db("shared");
        return [532, "Can't acquire lock: $res"] if $res;
    }

    $res = $wargs{code}->(%$margs);

    if ($wargs{lock_db}) {
        $self->_unlock_db;
    }

    $res;
}

sub begin {
    my ($self, %args) = @_;
    $self->_wrap(
        args => \%args,
        cleanup => 1,
        code => sub {
            my $dbh = $self->{_dbh};
            my $r = $dbh->selectrow_hashref("SELECT * FROM tx WHERE str_id=?",
                                            {}, $args{tx_id});
            return [409, "Another transaction with that ID exists", undef,
                    {rollback=>0}] if $r;

            # XXX check for limits

            $dbh->do("INSERT INTO tx (str_id, owner_id, summary, status, ".
                         "ctime) VALUES (?,?,?,?,?)", {},
                     $args{tx_id}, $args{client_token}//"", $args{summary}, "i",
                     $self->{_now},
                 ) or return [532, "db: Can't insert tx: ".$dbh->errstr];

            $self->{_tx_id} = $args{tx_id};
            [200, "OK"];
        },
    );
}

sub _call {
    my ($self, $calls) = @_;
    $self->_loop_calls('call', $calls);
}

sub call {
    my ($self, %args) = @_;

    my ($f, $args, $calls);
    $self->_wrap(
        args => \%args,
        # we allow calling call() during rollback, since a function can call
        # other function using call(), but we don't actually bother to save the
        # undo calls.
        tx_status => ["i", "d", "u", "a", "v", "e"],
        code => sub {
            my $res = $self->_loop_calls(
                'call', $args{calls} // [[$args{f}, $args{args}]],
                {sp=>$args{sp}},
            );
            if ($res) {
                return [532, $res];
            } else {
                return $self->{_res}; # function res was cached here
            }
        },
    );
}

sub commit {
    my ($self, %args) = @_;
    $self->_wrap(
        args => \%args,
        tx_status => ["i", "a"],
        code => sub {
            my $dbh = $self->{_dbh};
            my $tx  = $self->{_cur_tx};
            if ($tx->{status} eq 'a') {
                my $res = $self->_rollback;
                return [532, "Can't roll back: $res"] if $res;
                return [200, "Rolled back"];
            }
            $dbh->do("DELETE FROM call WHERE tx_ser_id=?", {}, $tx->{ser_id})
                or return [532, "db: Can't delete call: ".$dbh->errstr];
            $dbh->do("UPDATE tx SET status=?, commit_time=? WHERE ser_id=?",
                     {}, "C", $self->{_now}, $tx->{ser_id})
                or return [532, "db: Can't update tx status to committed: ".
                               $dbh->errstr];
            [200, "OK"];
        },
    );
}

sub _rollback {
    my ($self) = @_;
    $self->_loop_calls('rollback');
}

sub rollback {
    my ($self, %args) = @_;
    $self->_wrap(
        args => \%args,
        tx_status => ["i", "a"],
        code => sub {
            my $res = $self->_rollback;
            $res ? [532, $res] : [200, "OK"];
        },
    );
}

sub prepare {
    [501, "Not implemented"];
}

sub savepoint {
    [501, "Not yet implemented"];
}

sub release_savepoint {
    [501, "Not yet implemented"];
}

sub list {
    my ($self, %args) = @_;
    $self->_wrap2(
        args => \%args,
        code => sub {
            my $dbh = $self->{_dbh};
            my @wheres = ("1");
            my @params;
            if ($args{tx_id}) {
                push @wheres, "str_id=?";
                push @params, $args{tx_id};
            }
            if ($args{tx_status}) {
                push @wheres, "status=?";
                push @params, $args{tx_status};
            }
            my $sth = $dbh->prepare(
                "SELECT * FROM tx WHERE ".join(" AND ", @wheres).
                    " ORDER BY ctime, ser_id");
            $sth->execute(@params);
            my @res;
            while (my $row = $sth->fetchrow_hashref) {
                if ($args{detail}) {
                    push @res, {
                        tx_id         => $row->{str_id},
                        tx_status     => $row->{status},
                        tx_start_time => $row->{ctime},
                        tx_commit_time=> $row->{commit_time},
                        tx_summary    => $row->{summary},
                    };
                } else {
                    push @res, $row->{str_id};
                }
            }
            [200, "OK", \@res];
        },
    );
}

sub undo {
    my ($self, %args) = @_;

    # find latest committed tx
    unless ($args{tx_id}) {
        my $dbh = $self->{_dbh};
        my @row = $dbh->selectrow_array(
            "SELECT str_id FROM tx WHERE status='C' ".
                "ORDER BY commit_time DESC, ser_id DESC LIMIT 1");
        return [412, "There are no committed transactions to undo"] unless @row;
        $args{tx_id} = $row[0];
    }

    $self->_wrap(
        args => \%args,
        tx_status => ["C"],
        code => sub {
            my $res = $self->_loop_calls('undo');
            $res ? [532, $res] : [200, "OK"];
        },
    );
}

sub redo {
    my ($self, %args) = @_;

    # find first undone committed tx
    unless ($args{tx_id}) {
        my $dbh = $self->{_dbh};
        my @row = $dbh->selectrow_array(
            "SELECT str_id FROM tx WHERE status='U' ".
                "ORDER BY commit_time ASC, ser_id ASC LIMIT 1");
        return [412, "There are no undone transactions to redo"] unless @row;
        $args{tx_id} = $row[0];
    }

    $self->_wrap(
        args => \%args,
        tx_status => ["U"],
        code => sub {
            my $res = $self->_loop_calls('redo');
            $res ? [532, $res] : [200, "OK"];
        },
    );
}

sub _discard {
    my ($self, $which, %args) = @_;
    my $wmeth = $which eq 'one' ? '_wrap' : '_wrap2';
    $self->$wmeth(
        label => $which,
        args => \%args,
        tx_status => $which eq 'one' ? ['C','U','X'] : undef,
        code => sub {
            my $dbh = $self->{_dbh};
            my $sth;
            if ($which eq 'one') {
                $sth = $dbh->prepare("SELECT ser_id FROM tx WHERE str_id=?");
                $sth->execute($self->{_cur_tx}{str_id});
            } else {
                $sth = $dbh->prepare(
                    "SELECT ser_id FROM tx WHERE status IN ('C','U','X')");
                $sth->execute;
            }
            my @txs;
            while (my @row = $sth->fetchrow_array) {
                push @txs, $row[0];
            }
            if (@txs) {
                my $txs = join(",", @txs);
                $dbh->do("DELETE FROM tx WHERE ser_id IN ($txs)")
                    or return [532, "db: Can't delete tx: ".$dbh->errstr];
                $dbh->do(
                    "DELETE FROM undo_step WHERE call_id IN ".
                        "(SELECT id FROM call WHERE tx_ser_id IN ($txs))");
                $dbh->do("DELETE FROM call WHERE tx_ser_id IN ($txs)");
                $log->infof("[tm] discard tx: %s", \@txs);
            }
            [200, "OK"];
        },
    );
}

sub discard {
    my $self = shift;
    $self->_discard('one', @_);
}

sub discard_all {
    my $self = shift;
    $self->_discard('all', @_);
}

1;
# ABSTRACT: Transaction manager

=head1 SYNOPSIS

 # used by Perinci::Access::InProcess


=head1 DESCRIPTION

This class implements transaction and undo manager (TM), as specified by
L<Rinci::function::Transaction> and L<Riap::Transaction>. It is meant to be
instantiated by L<Perinci::Access::InProcess>, but will also be passed to
transactional functions to save undo/redo data.

It uses SQLite database to store transaction list and undo/redo data as well as
transaction data directory to provide trash_dir/tmp_dir for functions that
require it.


=head1 ATTRIBUTES

=head2 _tx_id

This is just a convenience so that methods that require tx_id will get the
default value from here if tx_id not specified in arguments.


=head1 METHODS

=head2 new(%args) => OBJ

Create new object. Arguments:

=over 4

=item * pa => OBJ

Perinci::Access::InProcess object. This is required by Perinci::Tx::Manager to
load/get functions when it wants to perform undo/redo/recovery.
Perinci::Access::InProcess conveniently require() the Perl modules and wraps the
functions.

=item * data_dir => STR (default C<~/.perinci/.tx>)

=item * max_txs => INT (default 1000)

Limit maximum number of transactions maintained by the TM, including all rolled
back and committed transactions, since they are still recorded in the database.
The default is 1000.

Not yet implemented.

After this limit is reached, cleanup will be performed to delete rolled back
transactions, and after that committed transactions.

=item * max_open_txs => INT (default 100)

Limit maximum number of open (in progress, aborted, prepared) transactions. This
exclude resolved transactions (rolled back and committed). The default is no
limit.

Not yet implemented.

After this limit is reached, starting a new transaction will fail.

=item * max_committed_txs => INT (default 100)

Limit maximum number of committed transactions that is recorded by the database.
This is equal to the number of undo steps that are remembered.

After this limit is reached, cleanup will automatically be performed so that
the oldest committed transactions are purged.

Not yet implemented.

=item * max_open_age => INT

Limit the maximum age of open transactions (in seconds). If this limit is
reached, in progress transactions will automatically be purged because it times
out.

Not yet implemented.

=item * max_committed_age => INT

Limit the maximum age of committed transactions (in seconds). If this limit is
reached, the old transactions will start to be purged.

Not yet implemented.

=back

=head2 $tx->get_trash_dir => RESP

=head2 $tx->get_tmp_dir => RESP

=head2 $tm->begin(%args) => RESP

Start a new transaction.

Arguments: tx_id (str, required, unless already supplied via _tx_id()), twopc
(bool, optional, currently must be false since distributed transaction is not
yet supported), summary (optional).

TM will create an entry for this transaction in its database.

=head2 $tm->call(%args) => RESP

Arguments: C<sp> (optional, savepoint name, must be unique for this transaction,
not yet implemented), C<f> (fully-qualified function name), C<args> (arguments
to function, hashref). Or, C<calls> (list of function calls, array, [[f1,
args1], ...], alternative to specifying C<f> and C<args>).

Call one or more functions inside the scope of a transaction, i.e. recording the
undo call for each function call before actually calling it, to allow for
rollback/undo/redo. TM will first check that each function supports transaction,
and return 412 if it does not.

(Note: if call is a dry-run call, or to a pure function [one that does not
produce side effects], you can just perform the function directly without using
TM.)

To call a single function, specify C<f> and C<args>. To call several functions,
supply C<calls>.

Note: special arguments (those started with dash, C<->) will be stripped from
function arguments by TM.

If response from function is not success, rollback() will be called.

On success, will return the result from the last function.

=head2 $tx->commit(%args) => RESP

Arguments: tx_id

=head2 $tx->rollback(%args) => RESP

Arguments: tx_id, sp (optional, savepoint name to rollback to a specific
savepoint only).

Currently rolling back to a savepoint is not implemented.

=head2 $tx->prepare(%args) => RESP

Currently will return 501 (not implemented). This TM does not support
distributed transaction.

Arguments: tx_id

=head2 $tx->savepoint(%args) => RESP

Arguments: tx_id, sp (savepoint name).

Currently not implemented.

=head2 $tx->release_savepoint(%args) => RESP

Arguments: tx_id, sp (savepoint name).

Currently not implemented.

=head2 $tx->undo(%args) => RESP

Arguments: tx_id

=head2 $tx->redo(%args) => RESP

Arguments: tx_id

=head2 $tx->list(%args) => RESP

List transactions. Return an array of results sorted by creation date (in
ascending order).

Arguments: B<detail> (bool, default 0, whether to return transaction records
instead of just a list of transaction ID's).

=head2 $tx->discard(%args) => RESP

Discard (forget) a committed transaction. The transaction will no longer be
undoable.

Arguments: tx_id

=head2 $tm->discard_all(%args) => RESP

Discard (forget) all committed transactions.


=head1 SEE ALSO

L<Riap::Transaction>

L<Perinci::Access::InProcess>

L<Rinci::function::Undo>

L<Rinci::function::Transaction>

=cut
