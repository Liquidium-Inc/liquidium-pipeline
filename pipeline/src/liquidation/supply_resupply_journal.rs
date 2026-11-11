//! Saves the bot's supply top-up transfers on disk, so a restart resumes the same send
//! instead of forgetting it or sending a second one.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::outbound::driver::OutboundRecord;

/// One saved top-up send per ledger, kept in a small JSON file next to the WAL database.
pub struct ResupplyJournal {
    path: PathBuf,
    records: Mutex<BTreeMap<String, OutboundRecord>>,
}

impl ResupplyJournal {
    /// Path of the journal that belongs to the WAL database at `db_path`.
    pub fn path_for_db(db_path: &str) -> PathBuf {
        PathBuf::from(format!("{db_path}.resupply.json"))
    }

    /// Loads the journal. A missing file is an empty journal; an unreadable one is an error,
    /// because it may hold a send that already ran.
    pub fn open(path: PathBuf, now: i64) -> Result<Self, String> {
        let mut records: BTreeMap<String, OutboundRecord> = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("resupply journal {} is unreadable: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(format!("could not read resupply journal {}: {e}", path.display())),
        };
        // A send saved before a restart may have gone out.
        for record in records.values_mut() {
            record.mark_uncertain(now);
        }
        Ok(Self {
            path,
            records: Mutex::new(records),
        })
    }

    pub fn get(&self, key: &str) -> Option<OutboundRecord> {
        self.records.lock().expect("resupply journal lock").get(key).cloned()
    }

    /// Stores or clears the record for `key` and writes the journal before returning.
    pub fn set(&self, key: &str, record: Option<OutboundRecord>) -> Result<(), String> {
        let mut records = self.records.lock().expect("resupply journal lock");
        match record {
            Some(record) => records.insert(key.to_string(), record),
            None => records.remove(key),
        };

        let bytes = serde_json::to_vec_pretty(&*records).map_err(|e| format!("resupply journal encode error: {e}"))?;
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes).map_err(|e| format!("could not write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("could not replace {}: {e}", self.path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbound::driver::OutboundPhase;
    use liquidium_pipeline_core::outbound::PreparedSend;

    fn record(replay_safe: bool) -> OutboundRecord {
        OutboundRecord::prepared(PreparedSend {
            sender: "icp".into(),
            identity: "id-1".into(),
            payload: "{}".into(),
            replay_safe,
        })
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("resupply-journal-{name}-{}.json", std::process::id()))
    }

    #[test]
    fn a_saved_send_survives_a_restart_and_a_cleared_one_does_not() {
        // given
        let path = temp_path("restart");
        let _ = std::fs::remove_file(&path);
        let journal = ResupplyJournal::open(path.clone(), 0).unwrap();
        journal.set("ledger-a", Some(record(true))).unwrap();
        journal.set("ledger-b", Some(record(true))).unwrap();
        journal.set("ledger-b", None).unwrap();

        // when
        let reopened = ResupplyJournal::open(path.clone(), 10).unwrap();

        // then
        assert_eq!(reopened.get("ledger-a"), Some(record(true)));
        assert_eq!(reopened.get("ledger-b"), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_send_that_is_not_safe_to_repeat_is_only_settled_after_a_restart() {
        // given
        let path = temp_path("uncertain");
        let _ = std::fs::remove_file(&path);
        ResupplyJournal::open(path.clone(), 0)
            .unwrap()
            .set("ledger-a", Some(record(false)))
            .unwrap();

        // when
        let reopened = ResupplyJournal::open(path.clone(), 42).unwrap();

        // then
        assert_eq!(
            reopened.get("ledger-a").unwrap().phase,
            OutboundPhase::Submitted { at: 42 }
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_unreadable_journal_is_an_error() {
        // given
        let path = temp_path("corrupt");
        std::fs::write(&path, b"not json").unwrap();

        // when / then
        assert!(ResupplyJournal::open(path.clone(), 0).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
