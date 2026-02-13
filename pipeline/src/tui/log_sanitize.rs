/// Removes ANSI escape/control sequences and non-printable control chars from
/// externally sourced log lines before rendering in ratatui.
pub(super) fn sanitize_log_line(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut out = String::with_capacity(input.len());

    while i < bytes.len() {
        if bytes[i] == 0x1b {
            skip_escape_sequence(bytes, &mut i);
            continue;
        }

        let mut chars = input[i..].chars();
        let Some(ch) = chars.next() else {
            break;
        };
        i += ch.len_utf8();

        if ch == '\t' {
            out.push(' ');
            continue;
        }
        if ch.is_control() {
            continue;
        }
        out.push(ch);
    }

    out
}

fn skip_escape_sequence(bytes: &[u8], i: &mut usize) {
    *i += 1;
    if *i >= bytes.len() {
        return;
    }

    match bytes[*i] {
        b'[' => {
            // CSI ... final-byte
            *i += 1;
            while *i < bytes.len() {
                let b = bytes[*i];
                *i += 1;
                if (0x40..=0x7e).contains(&b) {
                    break;
                }
            }
        }
        b']' => {
            // OSC ... BEL or ST (ESC \)
            *i += 1;
            while *i < bytes.len() {
                if bytes[*i] == 0x07 {
                    *i += 1;
                    break;
                }
                if bytes[*i] == 0x1b && *i + 1 < bytes.len() && bytes[*i + 1] == b'\\' {
                    *i += 2;
                    break;
                }
                *i += 1;
            }
        }
        b'P' | b'X' | b'^' | b'_' => {
            // DCS/SOS/PM/APC ... ST (ESC \)
            *i += 1;
            while *i < bytes.len() {
                if bytes[*i] == 0x1b && *i + 1 < bytes.len() && bytes[*i + 1] == b'\\' {
                    *i += 2;
                    break;
                }
                *i += 1;
            }
        }
        _ => {
            // Single-char escape sequence.
            *i += 1;
        }
    }
}

#[cfg(test)]
mod tests {
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
}
