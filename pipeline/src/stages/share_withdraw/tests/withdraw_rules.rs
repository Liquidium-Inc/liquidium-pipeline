//! Table tests of the pure decisions of the withdraw step.
use super::super::withdraw_rules::*;
use super::*;

/// One row for each case of each rule of `next_withdraw`. The least withdraw is 100.
#[test]
fn next_withdraw_follows_its_rules_in_order() {
    use WithdrawStep::*;
    let n = |value: u64| Nat::from(value);
    #[rustfmt::skip]
    let cases = [
        // (name, remaining, liquidity, deposited, only_row, step)
        ("rule 1: rest at the least withdraw", 100, 1_000_000, 100, true, Release),
        ("rule 1: nothing left", 0, 1_000_000, 0, true, Release),
        ("rule 2: no liquidity", 100_000, 0, 100_000, true, Wait(NO_FREE_LIQUIDITY)),
        ("rule 2: liquidity at the least withdraw", 100_000, 100, 100_000, true, Wait(NO_FREE_LIQUIDITY)),
        ("rule 3: empty position", 100_000, 1_000_000, 0, true, ParkEmptyPosition),
        ("rule 4: last row takes the interest", 100_000, 1_000_000, 100_050, true, Withdraw(n(100_050))),
        ("rule 4: interest exactly at 5%", 100_000, 1_000_000, 105_000, true, Withdraw(n(105_000))),
        ("rule 5: position above 5% keeps other shares", 100_000, 1_000_000, 105_001, true, Withdraw(n(100_000))),
        ("rule 5: last row, pool pays a part", 100_000, 40_000, 100_050, true, Withdraw(n(40_000))),
        ("rule 5: other rows in the pool", 100_000, 1_000_000, 130_000, false, Withdraw(n(100_000))),
        ("rule 5: position below the row amount", 100_000, 1_000_000, 60_000, false, Withdraw(n(60_000))),
        ("rule 6: part at the least withdraw", 100_000, 1_000_000, 100, false, Wait(BELOW_LEAST_WITHDRAW)),
    ];
    for (name, remaining, liquidity, deposited, only_row, step) in cases {
        let input = WithdrawInputs {
            remaining: n(remaining),
            min_withdraw: n(100),
            liquidity: n(liquidity),
            deposited: n(deposited),
            only_row,
        };
        assert_eq!(next_withdraw(&input), step, "{name}");
    }
}

/// One row for each rule of `after_failure`.
#[test]
fn after_failure_follows_its_rules_in_order() {
    let retry = |secs| move |result: AfterFailure| matches!(result, AfterFailure::Retry { secs: s, .. } if s == secs);
    let park = |result: AfterFailure| matches!(result, AfterFailure::Park(_));
    let unknown = |result: AfterFailure| result == AfterFailure::Unknown;
    let cases: Vec<(&str, WithdrawFailure, Box<dyn Fn(AfterFailure) -> bool>)> = vec![
        (
            "rule 1: certified reject",
            WithdrawFailure::NotExecuted("trap".to_string()),
            Box::new(retry(RETRY_SECS)),
        ),
        (
            "rule 2: no liquidity",
            WithdrawFailure::Refused(ProtocolError::NoLiquidity),
            Box::new(retry(REFUSED_RETRY_SECS)),
        ),
        (
            "rule 2: pool frozen",
            WithdrawFailure::Refused(ProtocolError::PoolFrozen),
            Box::new(retry(REFUSED_RETRY_SECS)),
        ),
        (
            "rule 2: health factor",
            WithdrawFailure::Refused(ProtocolError::HealthFactorTooLow),
            Box::new(retry(REFUSED_RETRY_SECS)),
        ),
        (
            "rule 3: wrong profile",
            WithdrawFailure::Refused(ProtocolError::InvalidTargetPrincipal),
            Box::new(park),
        ),
        (
            "rule 3: no funds",
            WithdrawFailure::Refused(ProtocolError::InsufficientFunds),
            Box::new(park),
        ),
        (
            "rule 4: transport error",
            WithdrawFailure::Ambiguous("timeout".to_string()),
            Box::new(unknown),
        ),
    ];
    for (name, failure, expected) in cases {
        assert!(expected(after_failure(&failure)), "{name}");
    }
}
