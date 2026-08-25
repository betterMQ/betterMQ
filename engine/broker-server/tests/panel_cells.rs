use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> std::net::SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    address
}

async fn wait_health(client: &reqwest::Client, url: &str) -> bool {
    for _ in 0..80 {
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn spawn_serve(args: &[&str], data_dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_bettermq"))
        .args(args)
        .env("BETTERMQ_INSECURE_NO_AUTH", "1")
        .env("BETTERMQ_CLUSTER_SECRET", "panel-cells-test-secret")
        .env("RUST_LOG", "error")
        .current_dir(data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

#[tokio::test]
async fn embedded_panel_persists_local_cell_and_federates_local_kinds() {
    let data = tempfile::tempdir().unwrap();
    let address = free_addr();
    let child = spawn_serve(
        &[
            "serve",
            "--listen",
            &address.to_string(),
            "--data-dir",
            data.path().to_str().unwrap(),
        ],
        data.path(),
    );
    let _guard = ChildGuard(child);
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert!(wait_health(&client, &format!("{base}/healthz")).await);

    let cells: serde_json::Value = client
        .get(format!("{base}/admin/v1/cells"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        cells["cells"].as_array().is_some_and(|c| !c.is_empty()),
        "local cell should be auto-registered: {cells}"
    );
    assert!(data.path().join("cell-registry.json").is_file());

    let fed: serde_json::Value = client
        .get(format!("{base}/admin/v1/cells/query/nodes"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        fed["ok"].as_array().is_some_and(|ok| !ok.is_empty()),
        "federated nodes should return local data: {fed}"
    );
    assert_ne!(fed["ok"][0]["data"], serde_json::json!({ "kind": "nodes" }));

    let upsert = client
        .post(format!("{base}/admin/v1/cells"))
        .json(&serde_json::json!({
            "id": "eu-west",
            "region": "eu-west",
            "controllerUrl": "http://10.0.0.9:8090",
            "label": "EU"
        }))
        .send()
        .await
        .unwrap();
    assert!(upsert.status().is_success(), "{}", upsert.status());
    let on_disk = std::fs::read_to_string(data.path().join("cell-registry.json")).unwrap();
    assert!(on_disk.contains("eu-west"));

    let cors = client
        .request(reqwest::Method::OPTIONS, format!("{base}/admin/v1/health"))
        .header("Origin", "http://example.invalid")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert!(
        cors.headers().get("access-control-allow-origin").is_none(),
        "default CORS must stay closed"
    );
}

#[tokio::test]
async fn dispatch_fleet_enroll_is_rejected() {
    let data = tempfile::tempdir().unwrap();
    let address = free_addr();
    let child = Command::new(env!("CARGO_BIN_EXE_bettermq"))
        .args([
            "serve",
            "--dispatch-fleet",
            "--listen",
            &address.to_string(),
            "--data-dir",
            data.path().to_str().unwrap(),
        ])
        .env("BETTERMQ_INSECURE_NO_AUTH", "1")
        .env("BETTERMQ_BROKER_URLS", "http://127.0.0.1:9")
        .env("BETTERMQ_CLUSTER_SECRET", "panel-cells-test-secret")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _guard = ChildGuard(child);
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert!(wait_health(&client, &format!("{base}/healthz")).await);

    let resp = client
        .post(format!("{base}/v1/infra/cluster/enroll"))
        .json(&serde_json::json!({
            "seed_url": "http://127.0.0.1:9",
            "join_token": "nope",
            "public_url": "http://127.0.0.1:9",
            "node_name": "fleet1"
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == reqwest::StatusCode::BAD_REQUEST || status == reqwest::StatusCode::NOT_FOUND,
        "dispatch/WAL-less enroll must not join a replica set: {status}"
    );
}

#[tokio::test]
async fn standalone_panel_has_no_infra_join() {
    let data = tempfile::tempdir().unwrap();
    let address = free_addr();
    let child = Command::new(env!("CARGO_BIN_EXE_bettermq"))
        .args([
            "panel",
            "--listen",
            &address.to_string(),
            "--data-dir",
            data.path().to_str().unwrap(),
        ])
        .env("BETTERMQ_INSECURE_NO_AUTH", "1")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _guard = ChildGuard(child);
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert!(wait_health(&client, &format!("{base}/healthz")).await);

    let join = client
        .post(format!("{base}/v1/infra/cluster/join"))
        .json(&serde_json::json!({
            "seed_url": "http://127.0.0.1:9",
            "join_token": "nope",
            "public_url": "http://127.0.0.1:9",
            "node_name": "x"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(join.status(), reqwest::StatusCode::NOT_FOUND);

    let attach = client
        .post(format!("{base}/admin/v1/attach"))
        .json(&serde_json::json!({
            "profile": "dispatch",
            "reachUrl": "http://10.0.0.8:8080",
            "advertiseUrl": "http://10.0.0.8:8080",
            "nodeName": "dispatch1",
            "cellId": "local"
        }))
        .send()
        .await
        .unwrap();
    assert!(
        attach.status().is_client_error() || attach.status().as_u16() == 502,
        "unreachable fleet attach must not look like a broker join: {}",
        attach.status()
    );
}
