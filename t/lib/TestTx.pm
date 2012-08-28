package TestTx;

use 5.010;
use strict;
use warnings;

our %SPEC;

our %vals;

$SPEC{unsetval} = {
    v => 1.1,
    summary => 'Unset variable value',
    args => {
        name => {
            schema => 'str*',
            req => 1,
        },
        value => {
            schema => 'any',
            req => 1,
        },
    },
    features => {
        tx => {v=>2},
        idempotent => 1,
    },
};
sub unsetval {
    my %args = @_;

    my $tx_action = $args{-tx_action} // '';
    my $name  = $args{name};
    defined($name) or return [400, "Please specify name"];

    if ($tx_action eq 'check_state') {
        my $fixed = !exists($vals{$name});
        return [304, "Fixed"] if $fixed;
        return [200, "Fixable", undef, {undo_actions=>[
            [setval => {name=>$name, value=>$vals{$name}}],
        ]}];
    } elsif ($tx_action eq 'fix_state') {
        delete $vals{$name};
        return [200, "Fixed"];
    }
    [400, "Invalid -tx_action"];
}

$SPEC{setval} = {
    v => 1.1,
    summary => 'Set variable value',
    args => {
        name => {
            schema => 'str*',
            req => 1,
        },
        value => {
            schema => 'any',
            req => 1,
        },
    },
    features => {
        tx => {v=>2},
        idempotent => 1,
    },
};
sub setval {
    my %args = @_;

    my $tx_action = $args{-tx_action} // '';
    my $name  = $args{name};
    defined($name) or return [400, "Please specify name"];
    my $value = $args{value};

    if ($tx_action eq 'check_state') {
        my $exists = exists($vals{$name});
        my $fixed  = $exists && (
            (!defined($value) && !defined($vals{$name})) ||
                (defined($value) && defined($vals{$name}) &&
                     $value eq $vals{$name}));
        return [304, "Fixed"] if $fixed;
        return [200, "Fixable", undef, {undo_actions=>[
            $exists ? [setval => {name=>$name, value=>$vals{$name}}] :
                [unsetval => {name=>$name}],
        ]}];
    } elsif ($tx_action eq 'fix_state') {
        $vals{$name} = $value;
        return [200, "Fixed"];
    }
    [400, "Invalid -tx_action"];
}

$SPEC{setvals} = {
    v => 1.1,
    summary => 'Set several variables values',
    args => {
        values => {
            schema => 'hash*',
        },
    },
    features => {
        tx => {v=>2},
        idempotent => 1,
    },
};
sub setvals {
    my %args = @_;

    my $tx_action = $args{-tx_action} // '';
    my $values = $args{values} or return [400, "Please specify values"];
    ref($values) eq 'HASH' or return [400, "Invalid values: not hash"];

    my @undo;
    my @do;

    for my $name (keys %$values) {
        my $value = $values->{$name};
        my $res = setval(name=>$name, value=>$value, -tx_action=>'check_state');
        return [$res->[0], "Can't check_state for '$name': $res->[1]"]
            unless $res->[0] == 200 || $res->[0] == 304;
        next if $res->[0] == 304;
        #use Data::Dump; dd $res;
        push @do, [setval => {name=>$name, value=>$value}];
        push @undo, @{$res->[3]{undo_actions}};
    }

    if (@do) {
        return [200, "Fixable", undef, {do_actions=>\@do,undo_actions=>\@undo}];
    } else {
        return [304, "Fixed"];
    }
}

$SPEC{emptyvals} = {
    v => 1.1,
    summary => 'Unset all variables',
    args => {
        values => {
            schema => 'hash*',
        },
    },
    features => {
        tx => {v=>2},
        idempotent => 1,
    },
};
sub emptyvals {
    my %args = @_;

    my $tx_action = $args{-tx_action} // '';

    return [331, "Are you sure you want to empty all values?"]
        unless $args{-confirm};

    if ($tx_action eq 'check_state') {
        my @undo;
        for my $name (keys %vals) {
            push @undo, [setval => {name=>$name, value=>$vals{$name}}];
        }
        if (@undo) {
            return [200, "Fixable", undef, {undo_actions=>\@undo}];
        } else {
            return [304, "Fixed"];
        }
    } elsif ($tx_action eq 'fix_state') {
        %vals = ();
        return [200, "Fixed"];
    }
    [400, "Invalid -tx_action"];
}

1;
