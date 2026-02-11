use std::fmt::{Debug, Display};

const CODE_HINTS: [&str; 4] = ["code=", "error_code=", "code:", "ErrorCode"];

/// Format errors while preserving code-like metadata when only `Debug` includes it.
pub fn format_with_code<E>(error: &E) -> String
where
    E: Display + Debug,
{
    let display = error.to_string();
    let debug = format!("{error:?}");

    // `String`/`&str` debug output adds quotes, which carry no extra metadata.
    if debug == display || debug == format!("{display:?}") {
        return display;
    }

    let display_has_code = CODE_HINTS.iter().any(|hint| display.contains(hint));
    let debug_has_code = CODE_HINTS.iter().any(|hint| debug.contains(hint));

    if debug_has_code && !display_has_code {
        format!("{display} ({debug})")
    } else {
        display
    }
}

#[cfg(test)]
mod tests {
    use super::format_with_code;

    #[derive(Debug)]
    struct DebugCodeOnlyError;

    impl std::fmt::Display for DebugCodeOnlyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("registry load failed")
        }
    }

    #[test]
    fn format_with_code_keeps_plain_string_unchanged() {
        let error = "plain error".to_string();
        assert_eq!(format_with_code(&error), "plain error");
    }

    #[test]
    fn format_with_code_appends_debug_when_code_is_only_in_debug() {
        struct Wrapper;

        impl std::fmt::Debug for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Wrapper { code: \"E_TOKEN_001\" }")
            }
        }

        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("registry load failed")
            }
        }

        let formatted = format_with_code(&Wrapper);
        assert!(formatted.contains("registry load failed"));
        assert!(formatted.contains("code"));
    }

    #[test]
    fn format_with_code_keeps_non_coded_debug_as_display() {
        let formatted = format_with_code(&DebugCodeOnlyError);
        assert_eq!(formatted, "registry load failed");
    }
}
