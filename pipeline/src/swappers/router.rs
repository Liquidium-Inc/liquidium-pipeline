use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

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

    fn pick_venue<'a>(&'a self, req: &SwapRequest) -> &'a Arc<dyn SwapVenue> {
        let v = self.venues.get(&req.venue_hint.clone().unwrap_or("kong".to_owned()));
        let v = v.expect(&format!("{:?} venue not found", req.venue_hint));
        v
    }

    pub async fn init(&self) {
        for venue in &self.venues {
            let _ = venue.1.init().await;
        }
    }

    pub async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        self.pick_venue(req).quote(req).await
    }

    pub async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        self.pick_venue(req).execute(req).await
    }
}

impl Default for SwapRouter {
    fn default() -> Self {
        Self::new()
    }
}
