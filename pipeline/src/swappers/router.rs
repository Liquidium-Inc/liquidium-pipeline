use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use tracing::instrument;

use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapRequest},
    swap_interface::SwapInterface,
};

#[async_trait]
pub trait SwapVenue: Send + Sync {
    fn venue_name(&self) -> &'static str;
    async fn init(&self) -> Result<(), String>;
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

#[async_trait]
impl SwapInterface for SwapRouter {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        // delegate to inherent method to avoid recursion
        SwapRouter::quote(self, req).await
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        SwapRouter::execute(self, req).await
    }
}

pub struct SwapRouter {
    venues: HashMap<String, Arc<dyn SwapVenue>>,
    default_venue: Option<String>,
}

impl SwapRouter {
    pub fn new() -> Self {
        Self {
            venues: HashMap::new(),
            default_venue: None,
        }
    }

    pub fn with_venue(mut self, venue: Arc<dyn SwapVenue>) -> Self {
        self.venues.insert(venue.venue_name().to_string(), venue);
        self
    }

    pub fn with_default_venue(mut self, venue: Arc<dyn SwapVenue>) -> Self {
        let name = venue.venue_name().to_string();
        self.venues.insert(name.clone(), venue);
        self.default_venue = Some(name);
        self
    }

    fn pick_venue<'a>(&'a self, req: &SwapRequest) -> Result<&'a Arc<dyn SwapVenue>, String> {
        let name = req
            .venue_hint
            .as_ref()
            .or(self.default_venue.as_ref())
            .ok_or_else(|| "no default swap venue configured".to_string())?;
        self.venues.get(name).ok_or_else(|| format!("{} venue not found", name))
    }

    #[instrument(name = "swap_router.init", skip_all, err)]
    pub async fn init(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for venue in &self.venues {
            if let Err(err) = venue.1.init().await {
                errors.push(format!("{}: {}", venue.0, err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("swap venue init errors: {}", errors.join("; ")))
        }
    }

    #[instrument(name = "swap_router.quote", skip_all, err, fields(pay = %req.pay_asset.symbol, receive = %req.receive_asset.symbol))]
    pub async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        self.pick_venue(req)?.quote(req).await
    }

    #[instrument(name = "swap_router.execute", skip_all, err, fields(pay = %req.pay_asset.symbol, receive = %req.receive_asset.symbol))]
    pub async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        self.pick_venue(req)?.execute(req).await
    }
}

impl Default for SwapRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestVenue;

    #[async_trait]
    impl SwapVenue for TestVenue {
        fn venue_name(&self) -> &'static str {
            "test"
        }

        async fn init(&self) -> Result<(), String> {
            Ok(())
        }

        async fn quote(&self, _req: &SwapRequest) -> Result<SwapQuote, String> {
            unreachable!()
        }

        async fn execute(&self, _req: &SwapRequest) -> Result<SwapExecution, String> {
            unreachable!()
        }
    }

    #[test]
    fn default_venue_is_registered_through_the_common_abstraction() {
        let router = SwapRouter::new().with_default_venue(Arc::new(TestVenue));

        assert_eq!(router.default_venue.as_deref(), Some("test"));
        assert!(router.venues.contains_key("test"));
    }
}
