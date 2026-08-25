use broker_storage::{restore_local_archive_until, scrub_local_archive, ArchiveToolReport};
use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage:\n  bettermq-archive scrub <archive-dir>\n  bettermq-archive restore <archive-dir> <destination> [--until-unix-ms N]\n  bettermq-archive scrub-s3 [prefix]\n  bettermq-archive restore-s3 <destination> [prefix] [--until-unix-ms N]\n\n--until-unix-ms restores only segments archived at or before that timestamp (PITR). S3 stays off the ACK path."
    );
    std::process::exit(2);
}

fn print_report(report: ArchiveToolReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize archive report")
    );
    if !report.errors.is_empty() {
        std::process::exit(1);
    }
}

fn take_until(raw: Vec<String>) -> (Vec<String>, Option<u128>) {
    let mut until = None;
    let mut positional = Vec::new();
    let mut iter = raw.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--until-unix-ms" {
            until = iter.next().and_then(|value| value.parse().ok());
        } else if let Some(value) = arg.strip_prefix("--until-unix-ms=") {
            until = value.parse().ok();
        } else {
            positional.push(arg);
        }
    }
    (positional, until)
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("scrub") => {
            let root = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            print_report(scrub_local_archive(&root));
        }
        Some("restore") => {
            let raw: Vec<String> = args.collect();
            let (positional, until) = take_until(raw);
            let root = PathBuf::from(positional.first().cloned().unwrap_or_else(|| usage()));
            let destination = PathBuf::from(positional.get(1).cloned().unwrap_or_else(|| usage()));
            print_report(restore_local_archive_until(&root, &destination, until));
        }
        #[cfg(feature = "s3")]
        Some("scrub-s3") => {
            let prefix = args.next().unwrap_or_else(|| "bettermq-archive".into());
            let store = broker_storage::open_archive_object_store_from_env()
                .unwrap_or_else(|error| panic!("open archive object store: {error}"));
            let runtime = tokio::runtime::Runtime::new().expect("archive tooling runtime");
            print_report(runtime.block_on(broker_storage::scrub_object_archive(store, &prefix)));
        }
        #[cfg(feature = "s3")]
        Some("restore-s3") => {
            let raw: Vec<String> = args.collect();
            let (positional, until) = take_until(raw);
            let destination = PathBuf::from(positional.first().cloned().unwrap_or_else(|| usage()));
            let prefix = positional
                .get(1)
                .cloned()
                .unwrap_or_else(|| "bettermq-archive".into());
            let store = broker_storage::open_archive_object_store_from_env()
                .unwrap_or_else(|error| panic!("open archive object store: {error}"));
            let runtime = tokio::runtime::Runtime::new().expect("archive tooling runtime");
            print_report(
                runtime.block_on(broker_storage::restore_object_archive_until(
                    store,
                    &prefix,
                    &destination,
                    until,
                )),
            );
        }
        _ => usage(),
    }
}
