use std::{ops::Deref, sync::Arc};

use candid::{Nat, Principal};
use ic_agent::Agent;
use icrc_ledger_agent::Icrc1Agent;
use lending_utils::types::assets::{Asset, Assets};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IcrcToken {
    // The ledger id
    pub ledger: Principal,
    /// icrc1:decimals
    pub decimals: u8,
    /// icrc1:name
    pub name: String,
    /// icrc1:symbol
    pub symbol: String,
    /// icrc1:fee (in the smallest units)
    pub fee: Nat,
}

impl From<(Assets, Principal)> for IcrcToken {
    fn from(value: (Assets, Principal)) -> Self {
        IcrcToken {
            ledger: value.1,
            decimals: value.0.decimals() as u8,
            name: value.0.symbol(),
            symbol: value.0.symbol(),
            fee: Nat::from(0u8), // TODO: Set this to the real value
        }
    }
}

impl IcrcToken {
    pub async fn from_principal(principal: Principal, agent: Arc<Agent>) -> Self {
        let agent = Icrc1Agent {
            agent: agent.deref().clone(),
            ledger_canister_id: principal,
        };

        let decimals = agent
            .decimals(icrc_ledger_agent::CallMode::Query)
            .await
            .expect("could not get decimals");
        let name = agent
            .name(icrc_ledger_agent::CallMode::Query)
            .await
            .expect("could not get name");
        let symbol = agent
            .symbol(icrc_ledger_agent::CallMode::Query)
            .await
            .expect("could not get symbol");
        let fee = agent
            .fee(icrc_ledger_agent::CallMode::Query)
            .await
            .expect("could not get fee");

        Self {
            decimals,
            fee,
            ledger: principal,
            name,
            symbol,
        }
    }
}
