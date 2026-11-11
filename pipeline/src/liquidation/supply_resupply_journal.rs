//! Saves the bot's supply top-up transfers on disk, so a restart resumes the same send
//! instead of forgetting it or sending a second one.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::outbound::driver::{OperatorDecision, OutboundRecord};

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
        let mut records = Self::read(&path)?;
        // A send saved before a restart may have gone out.
        for record in records.values_mut() {
            record.mark_uncertain(now);
        }
        Ok(Self {
            path,
            records: Mutex::new(records),
        })
    }

    fn read(path: &Path) -> Result<BTreeMap<String, OutboundRecord>, String> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("resupply journal {} is unreadable: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(e) => Err(format!("could not read resupply journal {}: {e}", path.display())),
        }
    }

    /// Reads the file again, so an operator decision recorded by the CLI reaches a running bot.
    pub fn reload(&self) -> Result<(), String> {
        let records = Self::read(&self.path)?;
        *self.records.lock().expect("resupply journal lock") = records;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<OutboundRecord> {
        self.records.lock().expect("resupply journal lock").get(key).cloned()
    }

    pub fn entries(&self) -> Vec<(String, OutboundRecord)> {
        let records = self.records.lock().expect("resupply journal lock");
        records
            .iter()
            .map(|(key, record)| (key.clone(), record.clone()))
            .collect()
    }

    /// Stores or clears the record for `key`. The change counts only once it is on disk: on an
    /// error the journal keeps its previous contents, in memory and in the file.
    ///
    /// The bot and the CLI both write this file. Each write holds a lock file, starts from
    /// what is on disk and changes one key, so neither process replaces the other's records.
    pub fn set(&self, key: &str, record: Option<OutboundRecord>) -> Result<(), String> {
        let mut records = self.records.lock().expect("resupply journal lock");
        let _file_lock = self.lock_file()?;

        let mut candidate = Self::read(&self.path)?;
        match record {
            Some(mut record) => {
                // Keep an operator decision the other process recorded for this same send.
                if record.decision.is_none()
                    && let Some(on_disk) = candidate.get(key)
                    && on_disk.prepared.identity == record.prepared.identity
                {
                    record.decision = on_disk.decision.clone();
                }
                candidate.insert(key.to_string(), record)
            }
            None => candidate.remove(key),
        };

        self.write(&candidate)?;
        *records = candidate;
        Ok(())
    }

    /// Records an operator decision on the send `identity` saved for `key`. Refused when the
    /// journal no longer holds that send, so a decision never lands on a newer one.
    pub fn decide(&self, key: &str, identity: &str, decision: OperatorDecision) -> Result<(), String> {
        let mut records = self.records.lock().expect("resupply journal lock");
        let _file_lock = self.lock_file()?;

        let mut candidate = Self::read(&self.path)?;
        let record = candidate
            .get_mut(key)
            .filter(|record| record.prepared.identity == identity)
            .ok_or_else(|| format!("send {identity} is no longer open; list the open top-ups again"))?;
        record.decide(decision);

        self.write(&candidate)?;
        *records = candidate;
        Ok(())
    }

    /// Takes the exclusive lock that orders writes between processes. Dropping it releases it.
    fn lock_file(&self) -> Result<File, String> {
        let path = self.path.with_extension("json.lock");
        let file = File::create(&path).map_err(|e| format!("could not open {}: {e}", path.display()))?;
        file.lock()
            .map_err(|e| format!("could not lock {}: {e}", path.display()))?;
        Ok(file)
    }

    /// Replaces the file through a synced temporary file, then syncs the directory entry.
    fn write(&self, records: &BTreeMap<String, OutboundRecord>) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(records).map_err(|e| format!("resupply journal encode error: {e}"))?;
        let tmp = self.path.with_extension(format!("json.{}.tmp", std::process::id()));
        let describe = |step: &str, e: std::io::Error| format!("could not {step} {}: {e}", self.path.display());

        let mut file = File::create(&tmp).map_err(|e| describe("create the temporary file for", e))?;
        file.write_all(&bytes).map_err(|e| describe("write", e))?;
        file.sync_all().map_err(|e| describe("sync", e))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| describe("replace", e))?;

        let dir = match self.path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| describe("sync the directory of", e))
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
    fn a_failed_write_changes_nothing_in_memory() {
        // given a journal whose file cannot be written
        let journal = ResupplyJournal::open(PathBuf::from("/nonexistent-dir/resupply.json"), 0).unwrap();

        // when
        let result = journal.set("ledger-a", Some(record(true)));

        // then a later pass does not find a record that never reached the disk
        assert!(result.is_err());
        assert_eq!(journal.get("ledger-a"), None);
    }

    #[test]
    fn reload_picks_up_a_decision_written_by_another_process() {
        // given a running bot's journal and a CLI that opens the same file
        let path = temp_path("reload");
        let _ = std::fs::remove_file(&path);
        let bot = ResupplyJournal::open(path.clone(), 0).unwrap();
        bot.set("ledger-a", Some(record(true))).unwrap();
        let cli = ResupplyJournal::open(path.clone(), 0).unwrap();
        cli.decide("ledger-a", "id-1", OperatorDecision::Absent).unwrap();

        // when
        bot.reload().unwrap();

        // then
        assert_eq!(bot.get("ledger-a").unwrap().decision, Some(OperatorDecision::Absent));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_write_keeps_what_another_process_wrote() {
        // given a bot journal, and a CLI that decides one send and the bot does not know yet
        let path = temp_path("two-writers");
        let _ = std::fs::remove_file(&path);
        let bot = ResupplyJournal::open(path.clone(), 0).unwrap();
        bot.set("ledger-a", Some(record(true))).unwrap();
        let cli = ResupplyJournal::open(path.clone(), 0).unwrap();
        cli.decide("ledger-a", "id-1", OperatorDecision::Absent).unwrap();

        // when the bot, still holding its older view, writes both keys
        bot.set("ledger-b", Some(record(true))).unwrap();
        bot.set("ledger-a", Some(record(true))).unwrap();

        // then the decision for that same send survives
        let reopened = ResupplyJournal::open(path.clone(), 0).unwrap();
        assert_eq!(
            reopened.get("ledger-a").unwrap().decision,
            Some(OperatorDecision::Absent)
        );
        assert_eq!(reopened.get("ledger-b"), Some(record(true)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_decision_for_a_send_that_was_replaced_is_refused() {
        // given the bot finished send id-1 and started another one for the same ledger
        let path = temp_path("stale-decision");
        let _ = std::fs::remove_file(&path);
        let bot = ResupplyJournal::open(path.clone(), 0).unwrap();
        let mut newer = record(true);
        newer.prepared.identity = "id-2".into();
        bot.set("ledger-a", Some(newer.clone())).unwrap();
        let cli = ResupplyJournal::open(path.clone(), 0).unwrap();

        // when the CLI decides the send it listed earlier
        let result = cli.decide("ledger-a", "id-1", OperatorDecision::Absent);

        // then the newer send is untouched
        assert!(result.is_err());
        assert_eq!(
            ResupplyJournal::open(path.clone(), 0).unwrap().get("ledger-a"),
            Some(newer)
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
