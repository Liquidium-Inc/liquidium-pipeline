diesel::table! {
    liquidation_results (liq_id, idx) {
        liq_id -> Text,
        idx -> Integer,
        status -> Integer,
        attempt -> Integer,
        created_at -> BigInt,
        updated_at -> BigInt,
        meta_json -> Text,
    }
}
