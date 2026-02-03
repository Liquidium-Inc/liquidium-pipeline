use std::collections::HashMap;
use std::sync::Mutex;

use candid::{Nat, Principal};

#[derive(Default)]
pub struct ApprovalState {
    allowances: Mutex<HashMap<(Principal, Principal), Nat>>,
}

impl ApprovalState {
    pub fn new() -> Self {
        Self {
            allowances: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_allowance(&self, ledger: Principal, spender: Principal, allowance: Nat) {
        if let Ok(mut lock) = self.allowances.lock() {
            lock.insert((ledger, spender), allowance);
        }
    }

    pub fn get_allowance(&self, ledger: Principal, spender: Principal) -> Option<Nat> {
        let lock = self.allowances.lock().ok()?;
        lock.get(&(ledger, spender)).cloned()
    }

    pub fn needs_approval(&self, ledger: Principal, spender: Principal, threshold: &Nat) -> bool {
        match self.get_allowance(ledger, spender) {
            Some(allowance) => allowance < *threshold,
            None => true,
        }
    }
}
