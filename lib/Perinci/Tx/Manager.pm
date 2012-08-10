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
# turn it is called by Perinci::Access::HTTP::Server without extra eval().

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
    $log->tracef("[txm] Initializing data dir %s ...", $data_dir);

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
    status CHAR(1) NOT NULL, -- i, a, C, U, R, u, d, X, (e) [uppercase=final]
    ctime REAL NOT NULL,
    commit_time REAL,
    last_step_id INTEGER, -- last processed step when rollback
    UNIQUE (str_id)
)
_

    # last_step_id is for the recovery process to avoid repeating all the
    # function calls when rollback failed in the middle. for example, tx1 has 3
    # calls each with 2 steps: c1(s1,s2), c2(s3,s4), c3(s5,s6). tx1 is being
    # rollbacked. txm executes c3, then c2, then crashes before calling c1.
    # since last_step_id is set to s3 at the end of calling c2, then during
    # recovery, rollback continues at before s3, which is c1.

    $dbh->do(<<_) or return "$ep create call: ". $dbh->errstr;
CREATE TABLE IF NOT EXISTS call (
    tx_ser_id INTEGER NOT NULL, -- refers tx(ser_id)
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ctime REAL NOT NULL,
    f TEXT NOT NULL,
    args TEXT NOT NULL
)
_
    $dbh->do(<<_) or return "$ep create undo_step: ".$dbh->errstr;
CREATE TABLE IF NOT EXISTS undo_step (
    call_id INT NOT NULL, -- refers call(id) OR subcall(id)
    -- seq INTEGER NOT NULL, -- uses ROWID instead, sqlite-specific
    name TEXT, -- for named savepoint
    ctime REAL NOT NULL,
    data BLOB NOT NULL
)
_
    $dbh->do(<<_) or return "$ep create redo_step: ".$dbh->errstr;
CREATE TABLE IF NOT EXISTS redo_step (
    call_id INT NOT NULL, -- refers txcall(id)
    -- seq INTEGER NOT NULL, -- uses ROWID instead, sqlite-specific
    ctime REAL NOT NULL,
    data BLOB NOT NULL
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
INSERT OR IGNORE INTO _meta VALUES ('v', '3')
_

    # deal with table structure changes
  UPDATE_SCHEMA:
    while (1) {
        my ($v) = $dbh->selectrow_array(
            "SELECT value FROM _meta WHERE name='v'");
        if ($v eq '1') {
            $dbh->begin_work;

            # add 'nest_level' column
            $dbh->do("ALTER TABLE call ADD COLUMN ".
                         "nest_level INTEGER NOT NULL DEFAULT 1");
            $dbh->do("UPDATE _meta SET value='2' WHERE name='v'")
                or return "$ep update v 1->2: ".$dbh->errstr;

            $dbh->commit;
        } elsif ($v eq '2') {
            $dbh->begin_work;

            # replace last_call_id column with last_step_id
            $dbh->do("CREATE TEMPORARY TABLE tx_backup (
    ser_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    str_id VARCHAR(200) NOT NULL,
    owner_id VARCHAR(64) NOT NULL,
    summary TEXT,
    status CHAR(1) NOT NULL, -- i, a, C, U, R, u, d, X, (e) [uppercase=final]
    ctime REAL NOT NULL,
    commit_time REAL,
    last_step_id INTEGER, -- last processed step when rollback
    UNIQUE (str_id)
)");
            $dbh->do("INSERT INTO tx_backup
    SELECT ser_id,str_id,owner_id,summary,status,ctime,commit_time,null FROM tx"
                 );
            $dbh->do("DROP TABLE tx");
            $dbh->do("CREATE TABLE tx (
    ser_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    str_id VARCHAR(200) NOT NULL,
    owner_id VARCHAR(64) NOT NULL,
    summary TEXT,
    status CHAR(1) NOT NULL, -- i, a, C, U, R, u, d, X, (e) [uppercase=final]
    ctime REAL NOT NULL,
    commit_time REAL,
    last_step_id INTEGER, -- last processed step when rollback
    UNIQUE (str_id)
)");
            $dbh->do("DROP TABLE tx_backup");

            # drop 'nest_level' column, turns out we don't need it
            $dbh->do("CREATE TEMPORARY TABLE call_backup(
    tx_ser_id INTEGER NOT NULL, -- refers tx(ser_id)
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ctime REAL NOT NULL,
    f TEXT NOT NULL,
    args TEXT NOT NULL
)");
            $dbh->do("INSERT INTO call_backup
    SELECT tx_ser_id,id,ctime,f,args FROM call");
            $dbh->do("DROP TABLE call");
            $dbh->do("CREATE TABLE call (
    tx_ser_id INTEGER NOT NULL, -- refers tx(ser_id)
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    ctime REAL NOT NULL,
    f TEXT NOT NULL,
    args TEXT NOT NULL
)");
            $dbh->do("INSERT INTO call SELECT * FROM call_backup");
            $dbh->do("DROP TABLE call_backup");
            $dbh->do("UPDATE _meta SET value='3' WHERE name='v'")
                or return "$ep update v 2->3: ".$dbh->errstr;
            $dbh->commit;

        } else {
            # already the latest schema version
            last UPDATE_SCHEMA;
        }
    }

    $self->{_dbh} = $dbh;
    $log->tracef("[txm] Data dir initialization finished");
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

# return undef on success, or an error string on failure
sub _rollback_or_undo_or_redo {
    my ($self, $which) = @_;

    # rollback, undo, and redo share a fair amount of code, mainly looping
    # through function calls, so we combine them here.

    die "BUG: 'which' must be rollback/undo/redo"
        unless $which =~ /\A(rollback|undo|redo)\z/;

    # this prevent endless loop in rollback, since we call functions when doing
    # rollback, and functions might call $tx->rollback too upon failure.
    return if $self->{_in_rollback} && $which eq 'rollback';
    local $self->{_in_rollback} = 1 if $which eq 'rollback';

    my $tx = $self->{_cur_tx};
    unless ($tx) {
        $log->warnf("[txm] _$which() called w/o transaction, probably a bug");
        return;
    }
    $log->tracef("[txm] $which tx #%d (%s) ...", $tx->{ser_id}, $tx->{str_id});

    my $dbh = $self->{_dbh};

    $self->_rollback_dbh;
    # we're now in sqlite autocommit mode, we use this mode for the following
    # reasons: 1) after we set Rtx status to 'a' or 'u' or 'd', we need other
    # clients to see this, so they do not try to add steps to it. also after
    # that, each function call will involve record_call() and record_step() that
    # are all separate sqltx's so each call/step can be recorded permanently in
    # sqldb.

    my (@calls, $i_call, $call);
    my $os  = $tx->{status};
    my $ns  = $which eq 'rollback' ? 'a' : $which eq 'undo' ? 'u' : 'd';
    my $oss = $which eq 'rollback' ? "'i','u','d'" :
        $which eq 'undo' ? "'C'" : "'U'";
    eval {
        $dbh->do("UPDATE tx SET status='$ns', last_step_id=NULL ".
                     "WHERE ser_id=? AND status IN ($oss)",
                 {}, $tx->{ser_id})
            or die "db: Can't update tx status $oss -> $ns: ".$dbh->errstr;

        # for safety, check once again if Rtx status is indeed updated
        my @r = $dbh->selectrow_array("SELECT status FROM tx WHERE ser_id=?",
                                      {}, $tx->{ser_id});
        die "Status incorrect ($r[0])" unless $r[0] eq $ns;

        # collect all steps and group them into calls

        my $t = $which eq 'redo' ? 'redo_step' : 'undo_step';
        my $lsi = $tx->{last_step_id};
        my $steps = $dbh->selectall_arrayref(join(
            "",
            "SELECT s.ROWID AS id, s.call_id AS call_id,",
            "  s.ctime AS ctime, s.data AS data FROM $t s ",
            "LEFT JOIN call c ON s.call_id=c.id WHERE c.tx_ser_id=? ",
            ($lsi ? "AND (s.ROWID<>$lsi AND ".
                 "s.ctime <= (SELECT ctime FROM $t WHERE id=$lsi))":""),
            "ORDER BY s.ctime, s.ROWID"),
                                            {}, $tx->{ser_id});
        $steps = [reverse @$steps] unless $which eq 'redo';
        my $ca;
        if (@$steps) {
            $ca = $dbh->selectall_arrayref(join(
                "",
                "SELECT id, f, args FROM call WHERE id IN (",
                join(",", map {$_->[1]} @$steps), ")"));
        } else {
            $ca = [];
        }
        my %ch;
        for (@$ca) {
            eval { $_->[2] = $json->decode($_->[2]) };
            die "Can't decode JSON for call id $_->[0]: $@" if $@;
            $ch{$_->[0]} = {f=>$_->[1], args=>$_->[2]};
        }
        while (1) {
            my @cs;
            last unless @$steps;
            my $cid = $steps->[0][1];
            while (@$steps && $steps->[0][1] == $cid) {
                eval { $steps->[0][3] = $json->decode($steps->[0][3]) };
                die "Can't decode JSON for step id $steps->[0][0]: $@" if $@;
                push @cs, {
                    id=>$steps->[0][0],
                    ctime=>$steps->[0][2], data=>$steps->[0][3],
                };
                shift @$steps;
            }
            push @calls, {
                id=>$cid, f=>$ch{$cid}{f}, args=>$ch{$cid}{args}, steps=>\@cs};
        }
        #$log->tracef("[txm] [$which] Calls to perform: %s", \@calls);

        # perform the calls

        $i_call = 0;
        for (@calls) {
            $call = $_;
            $i_call++;
            my $undo_data = [ map {$_->{data}} @{ $call->{steps} }];
            $log->tracef("[txm] [$which] Performing call %d/%d: %s(%s), ".
                           "undo_data: %s ...",
                         $i_call, scalar(@calls), $call->{f}, $call->{args},
                         $undo_data);
            my $res = $self->_get_func_and_meta($call->{f});
            die "Can't get func: $res->[0] - $res->[1]" unless $res->[0] == 200;
            my ($func, $meta) = @{$res->[2]};
            # XXX check meta whether func supports undo + transactional?
            $res = $func->(
                %{$call->{args}},
                -undo_action=>($which eq 'redo' ? 'redo' : 'undo'),
                -undo_data=>$undo_data,
                -tx_manager=>$self, -tx_call_id=>$call->{id},
                # the following special arg is just informative, so function
                # knows and can act more robust under rollback if it needs to
                -tx_action=>($which eq 'rollback' ? 'rollback' : undef),
            );
            $log->tracef("[txm] [$which] Call result: %s", $res);
            die "Call failed: $res->[0] - $res->[1]"
                unless $res->[0] == 200 || $res->[0] == 304;

            # update last_step_id so we don't have to repeat all steps when we
            # resume a failed rollback. error can be ignored here, i think.
            $dbh->do("UPDATE tx SET last_step_id=? WHERE ser_id=?", {},
                     $call->{steps}[0]{id}, $tx->{ser_id}) if
                         $which eq 'rollback' && @{$call->{steps}};
        }
        if ($which eq 'undo' || $which eq 'redo') {
            my $t = $which eq 'undo' ? 'undo_step' : 'redo_step';
            $dbh->do("DELETE FROM $t WHERE call_id IN ".
                         "(SELECT id FROM call WHERE tx_ser_id=?)",
                     {}, $tx->{ser_id})
                or die "db: Can't empty $t: ".$dbh->errstr;
        }
        my $fs = $which eq 'rollback' ? 'R' : $which eq 'undo' ? 'U' : 'C';
        $dbh->do("UPDATE tx SET status='$fs' WHERE ser_id=?", {}, $tx->{ser_id})
            or die "db: Can't set tx status to $fs: ".$dbh->errstr;

    };
    my $eval_err = $@;
    if ($eval_err) {
        my $errmsg = join("",
                          ($i_call ? "Call #$i_call/".scalar(@calls).
                               " (func $call->{f}): " : ""),
                          $eval_err);
        if ($which eq 'rollback') {
            # if failed during rolling back, we don't know what else to do. we
            # set Rtx status to X (inconsistent) and ignore it.
            $dbh->do("UPDATE tx SET status='X' ".
                         "WHERE ser_id=? AND status='a'",
                     {}, $tx->{ser_id});
            return $errmsg;
        } else {
            my $rbres = $self->rollback;
            if ($rbres->[0] == 200) {
                return $errmsg." (rolled back)";
            } else {
                return $errmsg." (rollback failed: $rbres->[0] - $rbres->[1])";
            }
        }
    }
    return;
}

# return undef on success, or an error string on failure
sub _recover_or_cleanup {
    my ($self, $which) = @_;

    # TODO clean old tx's tmp_dir & trash_dir.

    $log->tracef("[txm] Performing $which ...");

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

    $log->tracef("[txm] Finished $which");
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
    my ($r) = @_;
    my $s = $r->{status};
    my $ss =
        $s eq 'i' ? "still in-progress" :
            $s eq 'a' ? "aborted, further requests ignored until rolled back" :
                $s eq 'C' ? "already committed" :
                    $s eq 'R' ? "already rolled back" :
                        $s eq 'U' ? "already committed+undone" :
                            $s eq 'u' ? "undoing" :
                                $s eq 'd' ? "redoing" :
                                    $s eq 'X' ? "inconsistent" :
                                        "unknown (bug)";
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
        "[txm] -> %s(%s) label=%s",
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
        # on error, rollback sqlite tx and skip the rest
        if ($res->[0] >= 400) {
            $self->_rollback_dbh;
            $self->_rollback;
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
    $log->tracef("[txm] -> %s(%s)", $caller[3],
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
            return [409, "Another transaction with that ID exists"] if $r;

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

sub record_call {
    my ($self, %args) = @_;
    my @caller = caller(1);
    my ($f, $eargs);

    $self->_wrap(
        args => \%args,
        tx_status => ["i", "a"],
        hook_check_args => sub {
            #return [400, "Please specify f"]         unless $args{f};
            return [400, "Please specify args"]      unless $args{args};

            # status a is only allowed when we record steps during rollback
            my $cur_tx = $self->{_cur_tx};
            if ($cur_tx && $cur_tx->{status} eq 'a' && !$self->{_in_rollback}) {
                $self->_rollback_dbh;
                return __resp_tx_status($cur_tx);
            }

            my $f = $args{f} // $caller[3];
            # strip special arguments
            my %h;
            for (keys %{$args{args}}) {
                $h{$_} = $args{args}{$_} unless /^-/;
            }
            eval { $eargs = $json->encode(\%h) };
            return [400, "args data not serializable to JSON: $@"] if $@;

            return;
        },
        code => sub {
            my $dbh = $self->{_dbh};
            my $f = $args{f} // $caller[3];
            $dbh->do(
                "INSERT INTO call (tx_ser_id, ctime, f, args) ".
                    "VALUES (?,?,?,?)", {},
                $self->{_cur_tx}{ser_id}, $self->{_now}, $f, $eargs)
                or return [532, "db: Can't insert call: ".$dbh->errstr];
            return [200, "OK", $dbh->last_insert_id('','','','')];
        },
    );
}

sub record_undo_step {
    my $self = shift;
    $self->_record_step('undo', @_);
}

sub record_redo_step {
    my $self = shift;
    $self->_record_step('redo', @_);
}

sub _record_step {
    my ($self, $which, %args) = @_;
    die "BUG: which must be undo/redo" unless $which =~ /\A(un|re)do\z/;
    my $data;

    my $res = $self->_wrap(
        label => $which,
        args => \%args,
        hook_check_args => sub {
            $args{call_id} or return [400, "Please specify call_id"];
            $data = $args{data} or return [400, "Please specify data"];
            ref($data) eq 'ARRAY' or return [400, "data must be array"];
            eval { $data = $json->encode($data) };
            $@ and return [400, "step data not serializable to JSON: $@"];
            return;
        },
        tx_status => ["i", "a", "u", "d"],
        code => sub {
            my $dbh = $self->{_dbh};

            # status a is only allowed when we record steps during rollback
            my $cur_tx = $self->{_cur_tx};
            if ($cur_tx->{status} eq 'a' && !$self->{_in_rollback}) {
                $self->_rollback_dbh;
                return __resp_tx_status($cur_tx);
            }

            my $rc = $dbh->selectrow_hashref(
                "SELECT id FROM call WHERE id=?", {}, $args{call_id});
            return [400, "call_id does not exist in database"] unless $rc;

            $dbh->do("INSERT INTO ${which}_step (ctime, call_id, data) VALUES ".
                         "(?,?,?)", {}, $self->{_now}, $args{call_id}, $data)
                or return [532, "db: Can't insert step: ".$dbh->errstr];
            [200, "OK", $dbh->last_insert_id('','','','')];
        },
    );
    $res;
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
                return $res if $res;
                return [200, "Rolled back"];
            }
            $dbh->do("DELETE FROM redo_step WHERE call_id IN ".
                         "(SELECT id FROM call WHERE tx_ser_id=?)",
                     {}, $tx->{ser_id})
                or return [532, "db: Can't empty redo_step: ".$dbh->errstr];
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
    $self->_rollback_or_undo_or_redo('rollback');
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
            my $res = $self->_rollback_or_undo_or_redo('undo');
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
            my $res = $self->_rollback_or_undo_or_redo('redo');
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
                $log->infof("[txm] discard tx: %s", \@txs);
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

=head2 $tx->begin(%args) => RESP

Start a new transaction.

Arguments: tx_id (str, required, unless already supplied via _tx_id()), twopc
(bool, optional, currently must be false since distributed transaction is not
yet supported), summary (optional).

TM will create an entry for this transaction in its database.

=head2 $tx->record_call(%args) => RESP

Record a function call. This method needs to be called before function does any
step.

Arguments: tx_id, f (optional, function name, will be retrieved from caller(1)
if not specified, assuming the call is from the function itself), args
(required, function arguments).

TM will create an entry for this call in its database.

Return call ID in enveloped response.

=head2 $tx->record_undo_step(%args) => RESP

Record an undo step. This method needs to be called before performing a step.

Arguments: tx_id, call_id, data (an array).

=head2 $tx->record_redo_step(%args) => RESP

Record a redo step. This method needs to be called before performing an undo
step.

Arguments: tx_id, call_id, data (an array).

=head2 $tx->get_trash_dir => RESP

=head2 $tx->get_tmp_dir => RESP

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

=head2 $tx->discard_all(%args) => RESP

Discard (forget) all committed transactions.


=head1 SEE ALSO

L<Riap::Transaction>

L<Perinci::Access::InProcess>

L<Rinci::function::Undo>

L<Rinci::function::Transaction>

=cut
