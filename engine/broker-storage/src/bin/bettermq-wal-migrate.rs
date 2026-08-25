use broker_storage::{inspect_wal, migrate_v1_to_v2, rollback_v1_to_v2};
use std::path::PathBuf;

fn usage() -> ! {
    eprintln!(
        "usage:\n  bettermq-wal-migrate inspect <wal-dir>\n  \
         bettermq-wal-migrate migrate <v1-dir> <v2-dir> <shard-id>\n  \
         bettermq-wal-migrate rollback <v2-dir>"
    );
    std::process::exit(2);
}

fn main() {
    if let Err(error) = run() {
        eprintln!("migration failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let command = args.next().unwrap_or_else(|| usage());
    match command.to_string_lossy().as_ref() {
        "inspect" => {
            let path = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            if args.next().is_some() {
                usage();
            }
            println!("{}", serde_json::to_string_pretty(&inspect_wal(path)?)?);
        }
        "migrate" => {
            let source = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            let destination = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            let shard_id = args
                .next()
                .unwrap_or_else(|| usage())
                .to_string_lossy()
                .parse::<u32>()?;
            if args.next().is_some() {
                usage();
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&migrate_v1_to_v2(source, destination, shard_id)?)?
            );
        }
        "rollback" => {
            let destination = PathBuf::from(args.next().unwrap_or_else(|| usage()));
            if args.next().is_some() {
                usage();
            }
            println!("{}", rollback_v1_to_v2(destination)?.display());
        }
        _ => usage(),
    }
    Ok(())
}
