pub fn plain_logs_enabled() -> bool {
    cfg!(feature = "plain-logs")
}

pub fn human_output_enabled() -> bool {
    !plain_logs_enabled()
}

#[cfg(test)]
mod tests {
    use super::{human_output_enabled, plain_logs_enabled};

    #[test]
    fn output_modes_are_mutually_exclusive() {
        assert_ne!(plain_logs_enabled(), human_output_enabled());
    }
}
