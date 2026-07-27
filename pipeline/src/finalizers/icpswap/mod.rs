pub mod finalizer;
// Step 4 defines the adapter before Step 6 wires it into the live orchestrator;
// the binary target therefore cannot reach these paths yet.
#[allow(dead_code)]
mod multi_venue_adapter;

#[cfg(test)]
mod tests;
