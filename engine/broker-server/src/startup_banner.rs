//! Colored startup banner for interactive terminals (wordmark + URLs, then logs below).

use broker_config::ResolvedServeSettings;
use broker_storage::StorageMode;
use std::io::{IsTerminal, Write};

const BRAND: &str = "\x1b[38;2;31;71;240m";
const GREEN: &str = "\x1b[32m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn print(settings: &ResolvedServeSettings, storage: StorageMode) {
    if !std::io::stderr().is_terminal() {
        return;
    }

    let base = public_base_url(&settings.listen.to_string());
    let panel = format!("{base}/panel/");
    let docs = format!("{base}/docs");
    let data = settings.data_dir.display().to_string();
    let storage_label = format!("{storage:?}");
    let cluster = if settings.cluster_enabled {
        "on"
    } else {
        "off"
    };

    let mut out = std::io::stderr().lock();
    let _ = writeln!(out);
    print_logo(&mut out);
    let _ = writeln!(out);
    let _ = writeln!(out, "{DIM}{BOLD}     Http Messaging & Scheduling{RESET}");
    let _ = writeln!(out);
    let _ = writeln!(out, "  {GREEN}●{RESET} {BOLD}RUNNING{RESET}");
    let _ = writeln!(out);
    let _ = writeln!(out, "  {DIM}API{RESET}      {base}");
    let _ = writeln!(out, "  {DIM}Panel{RESET}    {panel}");
    let _ = writeln!(out, "  {DIM}Docs{RESET}     {docs}");
    let _ = writeln!(out, "  {DIM}Data{RESET}     {data}");
    let _ = writeln!(
        out,
        "  {DIM}Storage{RESET}  {storage_label}  ·  {DIM}cluster{RESET} {cluster}"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "{DIM}────────────────────────────────────────{RESET}");
    let _ = writeln!(out);
}

fn print_logo(out: &mut impl Write) {
    let _ = writeln!(out, "     better{BRAND}MQ{RESET}");
}

fn public_base_url(listen: &str) -> String {
    let (host, port) = match listen.rsplit_once(':') {
        Some((h, p)) => (h.trim_matches(['[', ']']), p),
        None => (listen, "8080"),
    };
    let host = match host {
        "0.0.0.0" | "::" | "" => "127.0.0.1",
        h if h.starts_with("::ffff:") => &h[7..],
        h => h,
    };
    format!("http://{host}:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_wildcard_bind_to_localhost() {
        assert_eq!(public_base_url("0.0.0.0:8080"), "http://127.0.0.1:8080");
        assert_eq!(public_base_url("127.0.0.1:9090"), "http://127.0.0.1:9090");
    }
}
