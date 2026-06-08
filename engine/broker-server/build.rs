fn main() {
    let panel =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../control-panel/index.html");
    assert!(
        panel.is_file(),
        "control-panel/index.html missing — panel is embedded at compile time"
    );
}
