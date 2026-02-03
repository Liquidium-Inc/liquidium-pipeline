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
    pub venues: HashMap<String, Arc<dyn SwapVenue>>,
}

impl SwapRouter {
    pub fn new() -> Self {
        Self { venues: HashMap::new() }
    }

    pub fn with_venue(mut self, name: &str, venue: Arc<dyn SwapVenue>) -> Self {
        self.venues.insert(name.to_string(), venue);
        self
    }

    fn pick_venue<'a>(&'a self, req: &SwapRequest) -> Result<&'a Arc<dyn SwapVenue>, String> {
        let name = req.venue_hint.clone().unwrap_or_else(|| "kong".to_owned());
        self.venues
            .get(&name)
            .ok_or_else(|| format!("{} venue not found", name))
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
