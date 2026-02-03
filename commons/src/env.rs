const CONFIG_DIR: &str = ".liquidium-pipeline";
const CONFIG_FILE: &str = "config.env";

pub fn load_env() {
    // Load .env from current directory first (highest priority)
    let _ = dotenv::dotenv();

    // Load ~/.liquidium-pipeline/config.env as defaults (won't overwrite existing)
    if let Ok(home) = std::env::var("HOME") {
        let config_path = format!("{}/{}/{}", home, CONFIG_DIR, CONFIG_FILE);
        let _ = dotenv::from_filename(config_path);
    }
}

pub fn load_env_from_paths(local_env: &std::path::Path, default_config: &std::path::Path) {
    let _ = dotenv::from_filename(local_env);
    let _ = dotenv::from_filename(default_config);
}

pub fn config_dir() -> String {
    if let Ok(home) = std::env::var("HOME") {
        format!("{}/{}", home, CONFIG_DIR)
    } else {
        CONFIG_DIR.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const TEST_PRIORITY_VAR: &str = "TEST_PRIORITY_VAR";
    const LOCAL_ONLY_VAR: &str = "LOCAL_ONLY_VAR";
    const DEFAULT_ONLY_VAR: &str = "DEFAULT_ONLY_VAR";

    #[test]
    fn test_local_env_takes_priority_over_default_config() {
        // given
        let prev_priority = std::env::var(TEST_PRIORITY_VAR).ok();
        let prev_local = std::env::var(LOCAL_ONLY_VAR).ok();
        let prev_default = std::env::var(DEFAULT_ONLY_VAR).ok();

        let temp_dir = std::env::temp_dir().join(format!("env_test_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let local_env_path = temp_dir.join(".env");
        let default_config_path = temp_dir.join("config.env");

        let mut local_env = std::fs::File::create(&local_env_path).unwrap();
        writeln!(local_env, "{}=from_local_env", TEST_PRIORITY_VAR).unwrap();
        writeln!(local_env, "{}=local_value", LOCAL_ONLY_VAR).unwrap();

        let mut default_config = std::fs::File::create(&default_config_path).unwrap();
        writeln!(default_config, "{}=from_default_config", TEST_PRIORITY_VAR).unwrap();
        writeln!(default_config, "{}=default_value", DEFAULT_ONLY_VAR).unwrap();

        // when
        load_env_from_paths(&local_env_path, &default_config_path);

        // then
        assert_eq!(
            std::env::var(TEST_PRIORITY_VAR).unwrap(),
            "from_local_env",
            "Local .env should take priority over default config"
        );
        assert_eq!(
            std::env::var(LOCAL_ONLY_VAR).unwrap(),
            "local_value",
            "Local-only var should be loaded"
        );
        assert_eq!(
            std::env::var(DEFAULT_ONLY_VAR).unwrap(),
            "default_value",
            "Default-only var should be loaded as fallback"
        );

        // cleanup environment variables
        restore_or_remove_env(TEST_PRIORITY_VAR, prev_priority);
        restore_or_remove_env(LOCAL_ONLY_VAR, prev_local);
        restore_or_remove_env(DEFAULT_ONLY_VAR, prev_default);

        // cleanup temp files
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    fn restore_or_remove_env(key: &str, prev: Option<String>) {
        unsafe {
            match prev {
                Some(val) => std::env::set_var(key, val),
                None => std::env::remove_var(key),
            }
        }
    }
}
