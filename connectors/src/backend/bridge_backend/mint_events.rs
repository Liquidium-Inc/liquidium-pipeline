//! Match the minter's successful ledger mint to its exact Ethereum event source.
//! The append-only audit API is authoritative; wallet deltas and helper receipts
//! cannot distinguish concurrent deposits or prove destination delivery.
use super::BridgeMintReceipt;
use crate::pipeline_agent::PipelineAgent;
use candid::{CandidType, Encode, IDLArgs, Nat, Principal, types::value::IDLValue};
use icrc_ledger_types::icrc1::account::Account;

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct DepositExpectation {
    pub amount: String,
    pub recipient: Account,
    pub token_symbol: String,
}

impl DepositExpectation {
    pub fn matches(&self, receipt: &BridgeMintReceipt) -> bool {
        self.amount == receipt.amount.0.to_string()
            && self.recipient == receipt.recipient
            && self.token_symbol.eq_ignore_ascii_case(&receipt.token_symbol)
    }
}

#[derive(CandidType, serde::Deserialize)]
struct Range {
    start: u64,
    length: u64,
}

fn field<'a>(value: &'a IDLValue, name: &str) -> Result<&'a IDLValue, String> {
    let IDLValue::Record(fields) = value else {
        return Err("minter event: expected record".into());
    };
    fields
        .iter()
        .find(|f| f.id.get_id() == candid::idl_hash(name))
        .map(|f| &f.val)
        .ok_or_else(|| format!("minter event missing {name}"))
}

pub(super) async fn count<A: PipelineAgent>(agent: &A, minter: &Principal) -> Result<u64, String> {
    let (total, _) = page(agent, minter, 0, 0).await?;
    Ok(total)
}

async fn page<A: PipelineAgent>(
    agent: &A,
    minter: &Principal,
    start: u64,
    length: u64,
) -> Result<(u64, Vec<IDLValue>), String> {
    let bytes = agent
        .call_query_raw(
            minter,
            "get_events",
            Encode!(&Range { start, length }).map_err(|e| e.to_string())?,
        )
        .await?;
    let args = IDLArgs::from_bytes(&bytes).map_err(|e| format!("decode minter events: {e}"))?;
    let value = args.args.first().ok_or("empty minter event response")?;
    let IDLValue::Nat64(total) = field(value, "total_event_count")? else {
        return Err("invalid minter event count".into());
    };
    let IDLValue::Vec(events) = field(value, "events")? else {
        return Err("invalid minter event page".into());
    };
    Ok((*total, events.clone()))
}

pub(super) fn minted_transaction(event: &IDLValue, hash: &str) -> Result<bool, String> {
    let IDLValue::Variant(payload) = field(event, "payload")? else {
        return Err("invalid minter event payload".into());
    };
    let tag = payload.0.id.get_id();
    if tag != candid::idl_hash("MintedCkEth") && tag != candid::idl_hash("MintedCkErc20") {
        return Ok(false);
    }
    let source = field(&payload.0.val, "event_source")?;
    let IDLValue::Text(transaction) = field(source, "transaction_hash")? else {
        return Err("invalid mint source hash".into());
    };
    // These handles originate from a single helper deposit call, not arbitrary
    // batched transactions. The minter emits Minted only after ledger success.
    Ok(transaction.eq_ignore_ascii_case(hash))
}

fn nat(value: &IDLValue, name: &str) -> Result<Nat, String> {
    match field(value, name)? {
        IDLValue::Nat(value) => Ok(value.clone()),
        _ => Err(format!("invalid {name} in minter event")),
    }
}

fn accepted_deposit(event: &IDLValue, hash: &str) -> Result<Option<(Nat, Nat, Account)>, String> {
    let IDLValue::Variant(payload) = field(event, "payload")? else {
        return Err("invalid minter event payload".into());
    };
    let tag = payload.0.id.get_id();
    if tag != candid::idl_hash("AcceptedDeposit") && tag != candid::idl_hash("AcceptedErc20Deposit") {
        return Ok(None);
    }
    let body = &payload.0.val;
    let IDLValue::Text(transaction) = field(body, "transaction_hash")? else {
        return Err("invalid accepted deposit hash".into());
    };
    if !transaction.eq_ignore_ascii_case(hash) {
        return Ok(None);
    }
    let IDLValue::Principal(owner) = field(body, "principal")? else {
        return Err("invalid deposit owner".into());
    };
    let subaccount = match field(body, "subaccount")? {
        IDLValue::None => None,
        IDLValue::Opt(value) => match value.as_ref() {
            IDLValue::Blob(bytes) => Some(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| "invalid deposit subaccount length")?,
            ),
            _ => return Err("invalid deposit subaccount".into()),
        },
        _ => return Err("invalid optional deposit subaccount".into()),
    };
    Ok(Some((
        nat(body, "log_index")?,
        nat(body, "value")?,
        Account {
            owner: *owner,
            subaccount,
        },
    )))
}

pub(super) async fn receipt<A: PipelineAgent>(
    agent: &A,
    minter: &Principal,
    mut start: u64,
    hash: &str,
) -> Result<Option<BridgeMintReceipt>, String> {
    let mut accepted = Vec::new();
    loop {
        let (total, events) = page(agent, minter, start, 1000).await?;
        for event in &events {
            if let Some(deposit) = accepted_deposit(event, hash)? {
                accepted.push(deposit);
            }
            if minted_transaction(event, hash)? {
                let IDLValue::Variant(payload) = field(event, "payload")? else {
                    unreachable!()
                };
                let body = &payload.0.val;
                let source = field(body, "event_source")?;
                let log_index = nat(source, "log_index")?;
                let (_, amount, recipient) = accepted
                    .iter()
                    .find(|(index, _, _)| *index == log_index)
                    .ok_or("mint event has no matching accepted deposit in the persisted event range")?;
                let token_symbol = if payload.0.id.get_id() == candid::idl_hash("MintedCkEth") {
                    "ckETH".to_string()
                } else if let IDLValue::Text(symbol) = field(body, "ckerc20_token_symbol")? {
                    symbol.clone()
                } else {
                    return Err("invalid minted token symbol".into());
                };
                return Ok(Some(BridgeMintReceipt {
                    transaction_hash: hash.to_string(),
                    log_index,
                    mint_block_index: nat(body, "mint_block_index")?,
                    amount: amount.clone(),
                    recipient: recipient.clone(),
                    token_symbol,
                }));
            }
        }
        if events.is_empty() {
            if start < total {
                return Err("minter returned an empty nonterminal event page".into());
            }
            return Ok(None);
        }
        start += events.len() as u64;
        if start >= total {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::types::{
        Label,
        value::{IDLField, VariantValue},
    };

    fn event(tag: &str, hash: &str) -> IDLValue {
        let record = |name: &str, value| {
            IDLValue::Record(vec![IDLField {
                id: Label::Named(name.into()),
                val: value,
            }])
        };
        record(
            "payload",
            IDLValue::Variant(VariantValue(
                Box::new(IDLField {
                    id: Label::Named(tag.into()),
                    val: record("event_source", record("transaction_hash", IDLValue::Text(hash.into()))),
                }),
                0,
            )),
        )
    }

    #[test]
    fn only_the_matching_successful_mint_completes_a_deposit() {
        assert!(!minted_transaction(&event("AcceptedErc20Deposit", "0x123"), "0x123").unwrap());
        assert!(!minted_transaction(&event("MintedCkErc20", "0x456"), "0x123").unwrap());
        assert!(minted_transaction(&event("MintedCkErc20", "0x123"), "0x123").unwrap());
        assert!(minted_transaction(&event("MintedCkEth", "0x123"), "0x123").unwrap());
    }

    #[derive(CandidType)]
    struct Event {
        timestamp: u64,
        payload: Payload,
    }

    #[derive(CandidType)]
    enum Payload {
        AcceptedErc20Deposit {
            transaction_hash: String,
            log_index: Nat,
            value: Nat,
            principal: Principal,
            subaccount: Option<Vec<u8>>,
        },
        MintedCkErc20 {
            event_source: Source,
            mint_block_index: Nat,
            ckerc20_token_symbol: String,
        },
    }

    #[derive(CandidType)]
    struct Source {
        transaction_hash: String,
        log_index: Nat,
    }

    #[derive(CandidType)]
    struct Page {
        events: Vec<Event>,
        total_event_count: u64,
    }

    fn accepted() -> Event {
        Event {
            timestamp: 0,
            payload: Payload::AcceptedErc20Deposit {
                transaction_hash: "0x123".into(),
                log_index: 7u64.into(),
                value: 123456u64.into(),
                principal: Principal::anonymous(),
                subaccount: Some(vec![3; 32]),
            },
        }
    }

    fn minted(hash: &str) -> Event {
        Event {
            timestamp: 1,
            payload: Payload::MintedCkErc20 {
                event_source: Source {
                    transaction_hash: hash.into(),
                    log_index: 7u64.into(),
                },
                mint_block_index: 99u64.into(),
                ckerc20_token_symbol: "ckUSDC".into(),
            },
        }
    }

    #[tokio::test]
    async fn accepted_and_unrelated_mints_stay_pending() {
        let mut agent = crate::pipeline_agent::MockPipelineAgent::new();
        agent.expect_call_query_raw().returning(|_, _, _| {
            candid::encode_one(Page {
                events: vec![accepted(), minted("0x456")],
                total_event_count: 2,
            })
            .map_err(|e| e.to_string())
        });
        assert!(
            receipt(&agent, &Principal::management_canister(), 0, "0x123")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn mint_matches_across_pages_and_after_observer_restart() {
        for _ in 0..2 {
            let mut agent = crate::pipeline_agent::MockPipelineAgent::new();
            agent.expect_call_query_raw().times(2).returning(|_, _, arg| {
                let range: Range = candid::decode_one(&arg).unwrap();
                let events = match range.start {
                    42 => vec![accepted(), minted("0x456")],
                    44 => vec![minted("0x123")],
                    other => panic!("unexpected cursor {other}"),
                };
                candid::encode_one(Page {
                    events,
                    total_event_count: 45,
                })
                .map_err(|e| e.to_string())
            });
            let result = receipt(&agent, &Principal::management_canister(), 42, "0x123")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.log_index, Nat::from(7u64));
            assert_eq!(result.amount, Nat::from(123456u64));
            let mut expected = DepositExpectation {
                amount: "123456".into(),
                recipient: result.recipient,
                token_symbol: "ckUSDC".into(),
            };
            assert!(expected.matches(&result));
            expected.amount = "123457".into();
            assert!(!expected.matches(&result));
            expected.amount = "123456".into();
            expected.token_symbol = "ckUSDT".into();
            assert!(!expected.matches(&result));
            expected.token_symbol = "ckUSDC".into();
            expected.recipient.subaccount = None;
            assert!(!expected.matches(&result));
            assert_eq!(result.mint_block_index, Nat::from(99u64));
            assert_eq!(
                result.recipient,
                Account {
                    owner: Principal::anonymous(),
                    subaccount: Some([3; 32])
                }
            );
        }
    }
}
