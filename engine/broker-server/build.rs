use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

fn main() {
    let panel_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../panel");
    let dist_index = panel_dir.join("dist/index.html");
    let src_dir = panel_dir.join("src");

    println!("cargo:rerun-if-changed={}", src_dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        panel_dir.join("package.json").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        panel_dir.join("package-lock.json").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        panel_dir.join("vite.config.ts").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        panel_dir.join("index.html").display()
    );

    if src_dir.is_dir() && should_rebuild(&src_dir, &panel_dir, &dist_index) {
        rebuild_panel(&panel_dir);
    }

    assert!(
        dist_index.is_file(),
        "engine/panel/dist/index.html missing — run `npm ci && npm run build` in engine/panel (Node 20+)"
    );
}

fn should_rebuild(src_dir: &Path, panel_dir: &Path, dist_index: &Path) -> bool {
    let Some(dist_mtime) = file_mtime(dist_index) else {
        return true;
    };
    newer_than(src_dir, dist_mtime)
        || file_mtime(&panel_dir.join("package.json")).is_some_and(|t| t > dist_mtime)
        || file_mtime(&panel_dir.join("package-lock.json")).is_some_and(|t| t > dist_mtime)
        || file_mtime(&panel_dir.join("vite.config.ts")).is_some_and(|t| t > dist_mtime)
        || file_mtime(&panel_dir.join("index.html")).is_some_and(|t| t > dist_mtime)
}

fn newer_than(dir: &Path, than: SystemTime) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if newer_than(&path, than) {
                return true;
            }
        } else if file_mtime(&path).is_some_and(|t| t > than) {
            return true;
        }
    }
    false
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    path.metadata().ok()?.modified().ok()
}

fn rebuild_panel(panel_dir: &Path) {
    let node = Command::new("node").arg("-v").output();
    let Ok(out) = node else {
        println!("cargo:warning=Node 20+ not found; using committed engine/panel/dist");
        return;
    };
    if !out.status.success() {
        println!("cargo:warning=Node 20+ not found; using committed engine/panel/dist");
        return;
    }
    let ver = String::from_utf8_lossy(&out.stdout);
    let major = ver
        .trim()
        .trim_start_matches('v')
        .split('.')
        .next()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    if major < 20 {
        println!("cargo:warning=Node {ver} is older than 20; using committed engine/panel/dist");
        return;
    }

    let npm = if panel_dir.join("node_modules").is_dir() {
        let status = Command::new("npm")
            .arg("run")
            .arg("build")
            .current_dir(panel_dir)
            .status();
        status.ok().filter(|s| s.success()).is_some()
    } else {
        let ci = Command::new("npm")
            .arg("ci")
            .current_dir(panel_dir)
            .status();
        if !ci.map(|s| s.success()).unwrap_or(false) {
            println!("cargo:warning=npm ci failed; using committed engine/panel/dist if present");
            return;
        }
        let status = Command::new("npm")
            .arg("run")
            .arg("build")
            .current_dir(panel_dir)
            .status();
        status.ok().filter(|s| s.success()).is_some()
    };
    if !npm {
        println!(
            "cargo:warning=panel npm build failed; using committed engine/panel/dist if present"
        );
    }
}
