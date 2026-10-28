use std::iter::Peekable;
use std::str::Chars;

/// Removes ANSI escape/control sequences and non-printable control chars from
/// externally sourced log lines before rendering in ratatui.
pub(super) fn sanitize_log_line(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\x1b' => skip_escape_sequence(&mut chars),
            '\t' => out.push(' '),
            ch if ch.is_control() => {}
            ch => out.push(ch),
        }
    }
    out
}

/// Skips one escape sequence after its ESC. It steps by character, so a
/// malformed sequence cannot cut a multibyte character in half.
fn skip_escape_sequence(chars: &mut Peekable<Chars<'_>>) {
    match chars.peek() {
        Some('[') => {
            // CSI ... final-byte
            chars.next();
            for c in chars.by_ref() {
                if ('\x40'..='\x7e').contains(&c) {
                    break;
                }
            }
        }
        Some(']' | 'P' | 'X' | '^' | '_') => {
            // OSC ends at BEL or ST (ESC \); DCS, SOS, PM and APC end at ST only.
            let bel_ends = chars.next() == Some(']');
            while let Some(c) = chars.next() {
                if (bel_ends && c == '\x07') || (c == '\x1b' && chars.next_if_eq(&'\\').is_some()) {
                    break;
                }
            }
        }
        Some(' '..='~') => {
            // Single-char escape sequence.
            chars.next();
        }
        // ESC before text or at the end: drop the ESC and keep the text.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::sanitize_log_line;

    #[test]
    fn strips_basic_ansi_color_sequences() {
        let input = "\x1b[2m2026-02-12T08:09:12.945321Z\x1b[0m \x1b[32mINFO\x1b[0m started";
        assert_eq!(sanitize_log_line(input), "2026-02-12T08:09:12.945321Z INFO started");
    }

    #[test]
    fn strips_mixed_style_sequences() {
        let input = "\x1b[1mliquidation.init\x1b[0m\x1b[2m:\x1b[0m ok";
        assert_eq!(sanitize_log_line(input), "liquidation.init: ok");
    }

    #[test]
    fn removes_control_chars_and_normalizes_tabs() {
        let input = "a\tb\x07c\x00d";
        assert_eq!(sanitize_log_line(input), "a bcd");
    }

    #[test]
    fn preserves_unicode_printable_text() {
        let input = "Δ profit ✅";
        assert_eq!(sanitize_log_line(input), "Δ profit ✅");
    }

    #[test]
    fn escape_before_a_multibyte_character_keeps_the_character() {
        // The audit's reproduction: the old byte walk panicked on this line.
        assert_eq!(sanitize_log_line("MEXC error: \x1bé"), "MEXC error: é");
        assert_eq!(sanitize_log_line("\x1b[31m\x1b✅ ok\x1b"), "✅ ok");
        assert_eq!(sanitize_log_line("\x1b]0;title\x1b\\Δ"), "Δ");
    }

    proptest! {
        #[test]
        fn any_line_sanitizes_to_printable_text(
            parts in proptest::collection::vec(
                prop_oneof![
                    any::<String>(),
                    "\x1b[\\[\\]PX^_ -~]?.{0,4}",
                    Just("\x1b".to_string()),
                ],
                0..8,
            )
        ) {
            let line = parts.concat();
            let clean = sanitize_log_line(&line);
            prop_assert!(!clean.chars().any(|c| c.is_control()), "{clean:?}");
        }
    }
}
