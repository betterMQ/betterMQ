use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn gateway_only_bootstrap_creates_no_data_files() {
    let data_parent = tempfile::tempdir().unwrap();
    let data_dir = data_parent.path().join("must-not-exist");
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_bettermq"))
        .args([
            "serve",
            "--gateway-only",
            "--listen",
            &address.to_string(),
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .env("BETTERMQ_BROKER_URLS", "http://127.0.0.1:9")
        .env("BETTERMQ_INSECURE_NO_AUTH", "1")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _guard = ChildGuard(child);

    let client = reqwest::Client::new();
    let health = format!("http://{address}/healthz");
    let mut started = false;
    for _ in 0..50 {
        if client
            .get(&health)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(started, "gateway-only server did not start");
    assert!(
        !data_dir.exists(),
        "gateway-only bootstrap created {}",
        data_dir.display()
    );
}
