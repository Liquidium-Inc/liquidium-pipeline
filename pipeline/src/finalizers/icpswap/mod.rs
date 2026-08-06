// The venue is complete and tested here, but nothing selects it yet: the daemon
// still finalizes through the hybrid route. The multi-venue planner is what gives
// this module a production caller, so until then its items are only reachable
// from tests. Mirrors the same marker on `dex_finalizer`.
#![allow(dead_code)]

pub mod finalizer;

#[cfg(test)]
mod tests;
