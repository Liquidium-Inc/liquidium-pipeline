diesel::table! {
    liquidation_results (liq_id) {
        liq_id -> Text,
        status -> Integer,
        attempt -> Integer,
        created_at -> BigInt,
        updated_at -> BigInt,
        meta_json -> Text,
    }
}
