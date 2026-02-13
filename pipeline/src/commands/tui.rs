use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct TuiOptions {
    pub sock_path: PathBuf,
    pub unit_name: String,
    pub log_file: Option<PathBuf>,
}

pub async fn run(opts: TuiOptions) -> anyhow::Result<()> {
    crate::tui::run(opts).await
}
