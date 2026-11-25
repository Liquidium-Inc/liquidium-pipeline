//! Checks on the two sides of a ledger transfer, for an account or for an ICP account identifier.
use candid::Principal;
use ic_ledger_types::{AccountIdentifier, Subaccount};
use liquidium_pipeline_connectors::backend::ledger_history_backend::LedgerParty;

const DEFAULT_SUBACCOUNT: [u8; 32] = [0; 32];

/// Returns the ICP account identifier of the default account of `owner`, as hex text.
pub fn default_account_id_hex(owner: Principal) -> String {
    AccountIdentifier::new(&owner, &Subaccount(DEFAULT_SUBACCOUNT)).to_hex()
}

/// True when `party` is the default account of `owner`.
pub fn is_default_account(party: &LedgerParty, owner: Principal) -> bool {
    match party {
        LedgerParty::Account(account) => {
            account.owner == owner && account.effective_subaccount() == &DEFAULT_SUBACCOUNT
        }
        LedgerParty::AccountId(hex) => *hex == default_account_id_hex(owner),
    }
}

/// True when `party` is an account of `owner`. An ICP account identifier matches only the default account.
pub fn is_owned_by(party: &LedgerParty, owner: Principal) -> bool {
    match party {
        LedgerParty::Account(account) => account.owner == owner,
        LedgerParty::AccountId(hex) => *hex == default_account_id_hex(owner),
    }
}

/// True when `party` is an account with a subaccount field. An ICP account identifier does not show one.
pub fn has_subaccount(party: &LedgerParty) -> bool {
    matches!(party, LedgerParty::Account(account) if account.subaccount.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use icrc_ledger_types::icrc1::account::Account;

    fn owner() -> Principal {
        Principal::from_text("42svn-2yaaa-aaaae-qfcsq-cai").unwrap()
    }

    fn account(subaccount: Option<[u8; 32]>) -> LedgerParty {
        LedgerParty::Account(Account {
            owner: owner(),
            subaccount,
        })
    }

    #[test]
    fn the_default_account_has_no_subaccount_or_a_zero_one() {
        assert!(is_default_account(&account(None), owner()));
        assert!(is_default_account(&account(Some([0; 32])), owner()));
        assert!(!is_default_account(&account(Some([7; 32])), owner()));
        assert!(!is_default_account(&account(None), Principal::anonymous()));
    }

    #[test]
    fn an_account_identifier_matches_only_the_default_account_of_the_owner() {
        let id = LedgerParty::AccountId(default_account_id_hex(owner()));
        assert!(is_default_account(&id, owner()));
        assert!(is_owned_by(&id, owner()));
        assert!(!is_owned_by(&id, Principal::anonymous()));
        assert!(!has_subaccount(&id));
    }

    #[test]
    fn an_account_with_a_subaccount_belongs_to_its_owner() {
        assert!(is_owned_by(&account(Some([7; 32])), owner()));
        assert!(has_subaccount(&account(Some([7; 32]))));
        assert!(!has_subaccount(&account(None)));
    }
}
