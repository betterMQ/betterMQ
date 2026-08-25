//! Role profiles must not open broker storage for panel/gateway/fleet dispatch.

use broker_config::{resolve_serve, Component, ServeOverrides};

#[test]
fn panel_profile_does_not_require_wal() {
    let settings = resolve_serve(
        None,
        &ServeOverrides {
            standalone_panel: true,
            ..ServeOverrides::default()
        },
    )
    .unwrap();
    assert!(!settings.opens_local_storage);
    assert!(settings.has(Component::Panel));
    assert!(settings.has(Component::Admin));
    assert!(!settings.has(Component::Broker));
}

#[test]
fn gateway_profile_does_not_require_wal() {
    let settings = resolve_serve(
        None,
        &ServeOverrides {
            gateway_only: Some(true),
            ..ServeOverrides::default()
        },
    )
    .unwrap();
    assert!(!settings.opens_local_storage);
    assert!(settings.has(Component::Gateway));
    assert!(!settings.has(Component::Broker));
}

#[test]
fn dispatch_fleet_profile_does_not_require_local_wal() {
    let settings = resolve_serve(
        None,
        &ServeOverrides {
            dispatch_fleet: Some(true),
            ..ServeOverrides::default()
        },
    )
    .unwrap();
    assert!(!settings.opens_local_storage);
    assert!(settings.has(Component::Dispatch));
    assert!(!settings.has(Component::Broker));
}

#[test]
fn broker_profile_opens_storage_without_local_dispatch() {
    let settings = resolve_serve(
        None,
        &ServeOverrides {
            broker_only: Some(true),
            ..ServeOverrides::default()
        },
    )
    .unwrap();
    assert!(settings.opens_local_storage);
    assert!(settings.has(Component::Broker));
    assert!(!settings.has(Component::Dispatch));
}
