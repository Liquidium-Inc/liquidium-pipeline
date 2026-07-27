use candid::{CandidType, Nat};
use liquidium_pipeline_core::tokens::{asset_id::AssetId, chain_token_amount::ChainTokenAmount};
use serde::{Deserialize, Serialize};

const BPS_PER_RATIO_UNIT: f64 = 10_000.0;

/// Normalized adverse quote impact for prices expressed in receive units per
/// pay unit. Better-than-reference quotes are clamped to zero.
pub fn adverse_price_impact_bps(reference_price: f64, execution_price: f64) -> f64 {
    if !reference_price.is_finite() || reference_price <= 0.0 || !execution_price.is_finite() || execution_price < 0.0 {
        return f64::INFINITY;
    }
    ((reference_price - execution_price) / reference_price * BPS_PER_RATIO_UNIT).max(0.0)
}

#[derive(CandidType, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxRef {
    IcBlockIndex { ledger: String, block_index: Nat },
    TxHash { chain: String, hash: String },
}

#[derive(CandidType, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapRequest {
    pub pay_asset: AssetId,
    /// Maximum amount the venue may spend, including venue-required ledger operations.
    pub pay_amount: ChainTokenAmount,
    pub receive_asset: AssetId,
    pub receive_address: Option<String>,
    pub max_slippage_bps: Option<u32>, // 100 = 1%
    pub venue_hint: Option<String>,    // "icpswap", "mexc", etc (optional)
}

#[derive(CandidType, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapQuoteLeg {
    pub venue: String,    // "icpswap", "mexc", etc
    pub route_id: String, // pool id, market symbol, etc

    pub pay_chain: String,
    pub pay_symbol: String,
    pub pay_amount: Nat,

    pub receive_chain: String,
    pub receive_symbol: String,
    pub receive_amount: Nat,

    pub price: f64,
    pub lp_fee: Nat,
    pub gas_fee: Nat,
}

#[derive(CandidType, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapQuote {
    pub pay_asset: AssetId,
    pub pay_amount: Nat,
    pub receive_asset: AssetId,
    pub receive_amount: Nat,
    /// Venue reference price before size impact, expressed as receive units per
    /// pay unit. DEX adapters use pool spot; CEX adapters use the best side.
    pub mid_price: f64,
    /// Expected amount-scoped execution price in receive units per pay unit.
    /// For a CEX this is the order-book VWAP, normalized for trade direction.
    pub exec_price: f64,
    /// Adverse difference between `mid_price` and `exec_price`. This is known at
    /// quote time and is distinct from drift between quote and actual execution.
    #[serde(default, alias = "estimated_slippage_bps", alias = "slippage")]
    pub estimated_price_impact_bps: f64,

    pub legs: Vec<SwapQuoteLeg>,
}

#[derive(CandidType, Debug, Clone, Serialize, Deserialize)]
pub struct TransferRecord {
    pub asset: AssetId,
    pub is_send: bool,
    pub amount: Nat,
    pub tx_ref: TxRef,
}

#[derive(CandidType, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwapExecution {
    pub swap_id: u64,
    pub request_id: u64,
    pub status: String,

    pub pay_asset: AssetId,
    pub pay_amount: Nat,
    pub receive_asset: AssetId,
    pub receive_amount: Nat,

    pub mid_price: f64,
    pub exec_price: f64,
    #[serde(default, alias = "slippage")]
    pub realized_slippage_bps: f64,

    pub legs: Vec<SwapQuoteLeg>,
    #[serde(default)]
    pub approval_count: Option<u32>,
    pub ts: u64,
}

#[cfg(test)]
mod tests {
    use candid::Nat;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};

    use super::*;

    fn request() -> SwapRequest {
        let pay_token = ChainToken::EvmNative {
            chain: "test".to_string(),
            symbol: "PAY".to_string(),
            decimals: 8,
            fee: Nat::from(1u8),
        };
        let receive_asset = AssetId {
            chain: "test".to_string(),
            address: "receive".to_string(),
            symbol: "RECEIVE".to_string(),
        };

        SwapRequest {
            pay_asset: pay_token.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(pay_token, Nat::from(100u64)),
            receive_asset,
            receive_address: None,
            max_slippage_bps: Some(100),
            venue_hint: None,
        }
    }

    fn quote() -> SwapQuote {
        let request = request();
        SwapQuote {
            pay_asset: request.pay_asset,
            pay_amount: request.pay_amount.value,
            receive_asset: request.receive_asset,
            receive_amount: Nat::from(200u64),
            mid_price: 2.0,
            exec_price: 1.99,
            estimated_price_impact_bps: 50.0,
            legs: Vec::new(),
        }
    }

    fn execution() -> SwapExecution {
        let quote = quote();
        SwapExecution {
            swap_id: 1,
            request_id: 2,
            status: "completed".to_string(),
            pay_asset: quote.pay_asset,
            pay_amount: quote.pay_amount,
            receive_asset: quote.receive_asset,
            receive_amount: quote.receive_amount,
            mid_price: quote.mid_price,
            exec_price: quote.exec_price,
            realized_slippage_bps: 75.0,
            legs: Vec::new(),
            approval_count: None,
            ts: 3,
        }
    }

    #[test]
    fn quote_reads_previous_price_impact_name_and_writes_normalized_name() {
        let mut encoded = serde_json::to_value(quote()).expect("serialize quote");
        let object = encoded.as_object_mut().expect("quote object");
        let value = object
            .remove("estimated_price_impact_bps")
            .expect("new price-impact field");
        object.insert("estimated_slippage_bps".to_string(), value);

        let decoded: SwapQuote = serde_json::from_value(encoded).expect("decode legacy quote");
        assert_eq!(decoded.estimated_price_impact_bps, 50.0);

        let reencoded = serde_json::to_value(decoded).expect("serialize normalized quote");
        assert_eq!(reencoded["estimated_price_impact_bps"], 50.0);
        assert!(reencoded.get("estimated_slippage_bps").is_none());
    }

    #[test]
    fn quote_still_reads_legacy_unqualified_slippage_name() {
        let mut encoded = serde_json::to_value(quote()).expect("serialize quote");
        let object = encoded.as_object_mut().expect("quote object");
        let value = object
            .remove("estimated_price_impact_bps")
            .expect("new price-impact field");
        object.insert("slippage".to_string(), value);

        let decoded: SwapQuote = serde_json::from_value(encoded).expect("decode legacy quote");
        assert_eq!(decoded.estimated_price_impact_bps, 50.0);
    }

    #[test]
    fn execution_reads_legacy_slippage_and_writes_basis_point_name() {
        let mut encoded = serde_json::to_value(execution()).expect("serialize execution");
        let object = encoded.as_object_mut().expect("execution object");
        let value = object.remove("realized_slippage_bps").expect("new slippage field");
        object.insert("slippage".to_string(), value);

        let decoded: SwapExecution = serde_json::from_value(encoded).expect("decode legacy execution");
        assert_eq!(decoded.realized_slippage_bps, 75.0);

        let reencoded = serde_json::to_value(decoded).expect("serialize normalized execution");
        assert_eq!(reencoded["realized_slippage_bps"], 75.0);
        assert!(reencoded.get("slippage").is_none());
    }
}
