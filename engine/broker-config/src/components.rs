//! Versioned component profiles, listeners, and panel modes.
//!
//! Config files are not rewritten on load. V1 files resolve to the same
//! in-memory [`ComponentSet`] as an explicit V2 `profile: "all"` document.

use crate::types::{BetterMqConfig, ConfigError};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub const CONFIG_VERSION_V1: u32 = 1;
pub const CONFIG_VERSION_V2: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Component {
    Broker,
    Controller,
    Dispatch,
    Gateway,
    Admin,
    Panel,
}

impl Component {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Controller => "controller",
            Self::Dispatch => "dispatch",
            Self::Gateway => "gateway",
            Self::Admin => "admin",
            Self::Panel => "panel",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "broker" => Ok(Self::Broker),
            "controller" => Ok(Self::Controller),
            "dispatch" => Ok(Self::Dispatch),
            "gateway" => Ok(Self::Gateway),
            "admin" => Ok(Self::Admin),
            "panel" => Ok(Self::Panel),
            other => Err(ConfigError::Invalid(format!(
                "unknown component '{other}' (use broker, controller, dispatch, gateway, admin, panel)"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComponentProfile {
    All,
    Broker,
    Dispatch,
    Gateway,
    Controller,
    Panel,
}

impl ComponentProfile {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Ok(Self::All),
            "broker" => Ok(Self::Broker),
            "dispatch" => Ok(Self::Dispatch),
            "gateway" => Ok(Self::Gateway),
            "controller" => Ok(Self::Controller),
            "panel" => Ok(Self::Panel),
            other => Err(ConfigError::Invalid(format!(
                "unknown profile '{other}' (use all, broker, dispatch, gateway, controller, panel)"
            ))),
        }
    }

    pub fn components(self) -> ComponentSet {
        match self {
            Self::All => ComponentSet::all(),
            Self::Broker => ComponentSet::from_slice(&[
                Component::Broker,
                Component::Controller,
                Component::Admin,
            ]),
            Self::Dispatch => ComponentSet::from_slice(&[Component::Dispatch]),
            Self::Gateway => ComponentSet::from_slice(&[Component::Gateway, Component::Admin]),
            Self::Controller => {
                ComponentSet::from_slice(&[Component::Controller, Component::Admin])
            }
            Self::Panel => ComponentSet::from_slice(&[Component::Admin, Component::Panel]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSpec {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub include: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum PanelMode {
    #[default]
    Embedded,
    SeparateListener,
    Disabled,
    Standalone,
}

impl PanelMode {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "embedded" => Ok(Self::Embedded),
            "separate" | "separate-listener" | "separatelistener" => Ok(Self::SeparateListener),
            "disabled" | "off" | "none" => Ok(Self::Disabled),
            "standalone" => Ok(Self::Standalone),
            other => Err(ConfigError::Invalid(format!(
                "unknown panel.mode '{other}' (use embedded, separate, disabled, standalone)"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PanelSection {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub listen: Option<String>,
    #[serde(default)]
    pub controller: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListenerSection {
    #[serde(default)]
    pub public: Option<String>,
    #[serde(default)]
    pub admin: Option<String>,
    #[serde(default)]
    pub internal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationPolicyConfig {
    #[serde(default = "default_rf")]
    pub factor: u32,
    #[serde(default = "default_min_isr")]
    pub min_isr: u32,
}

fn default_rf() -> u32 {
    3
}

fn default_min_isr() -> u32 {
    2
}

impl Default for ReplicationPolicyConfig {
    fn default() -> Self {
        Self {
            factor: default_rf(),
            min_isr: default_min_isr(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentSet {
    bits: u8,
}

impl ComponentSet {
    const BROKER: u8 = 1 << 0;
    const CONTROLLER: u8 = 1 << 1;
    const DISPATCH: u8 = 1 << 2;
    const GATEWAY: u8 = 1 << 3;
    const ADMIN: u8 = 1 << 4;
    const PANEL: u8 = 1 << 5;

    pub fn empty() -> Self {
        Self { bits: 0 }
    }

    pub fn all() -> Self {
        Self::from_slice(&[
            Component::Broker,
            Component::Controller,
            Component::Dispatch,
            Component::Gateway,
            Component::Admin,
            Component::Panel,
        ])
    }

    pub fn from_slice(items: &[Component]) -> Self {
        let mut set = Self::empty();
        for item in items {
            set.insert(*item);
        }
        set
    }

    pub fn insert(&mut self, component: Component) {
        self.bits |= bit(component);
    }

    pub fn contains(&self, component: Component) -> bool {
        self.bits & bit(component) != 0
    }

    pub fn iter(self) -> impl Iterator<Item = Component> {
        [
            Component::Broker,
            Component::Controller,
            Component::Dispatch,
            Component::Gateway,
            Component::Admin,
            Component::Panel,
        ]
        .into_iter()
        .filter(move |component| self.contains(*component))
    }

    pub fn requires_local_storage(&self) -> bool {
        self.contains(Component::Broker) || self.contains(Component::Controller)
    }

    pub fn requires_admin(&self) -> bool {
        self.contains(Component::Panel)
    }

    pub fn validate_dependencies(&self) -> Result<(), ConfigError> {
        if self.requires_admin() && !self.contains(Component::Admin) {
            return Err(ConfigError::Invalid(
                "panel requires the admin component".into(),
            ));
        }
        if self.contains(Component::Dispatch)
            && !self.contains(Component::Broker)
            && !self.contains(Component::Gateway)
        {
            // Fleet dispatch talks to remote brokers; local WAL is not required.
        }
        Ok(())
    }
}

fn bit(component: Component) -> u8 {
    match component {
        Component::Broker => ComponentSet::BROKER,
        Component::Controller => ComponentSet::CONTROLLER,
        Component::Dispatch => ComponentSet::DISPATCH,
        Component::Gateway => ComponentSet::GATEWAY,
        Component::Admin => ComponentSet::ADMIN,
        Component::Panel => ComponentSet::PANEL,
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedListeners {
    pub public: SocketAddr,
    pub admin: SocketAddr,
    pub internal: SocketAddr,
    pub collapsed: bool,
}

impl ResolvedListeners {
    pub fn validate_exposure(&self, components: ComponentSet) -> Result<(), ConfigError> {
        if self.collapsed {
            return Ok(());
        }
        if (components.contains(Component::Controller) || components.contains(Component::Broker))
            && is_unspecified_public(&self.internal)
            && std::env::var("BETTERMQ_SAAS").ok().as_deref() == Some("1")
        {
            return Err(ConfigError::Invalid(
                "internal listener must not bind a public unspecified address in SaaS mode".into(),
            ));
        }
        Ok(())
    }
}

fn is_unspecified_public(addr: &SocketAddr) -> bool {
    match addr {
        SocketAddr::V4(v4) => v4.ip().is_unspecified() && !v4.ip().is_loopback(),
        SocketAddr::V6(v6) => v6.ip().is_unspecified() && !v6.ip().is_loopback(),
    }
}

pub fn resolve_component_set(
    cfg: &BetterMqConfig,
    profile_override: Option<&str>,
    include_override: &[String],
    broker_only: bool,
    gateway_only: bool,
    dispatch_fleet: bool,
    standalone_panel: bool,
) -> Result<ComponentSet, ConfigError> {
    if standalone_panel {
        return Ok(ComponentProfile::Panel.components());
    }

    if let Some(profile) = profile_override {
        let mut set = ComponentProfile::parse(profile)?.components();
        apply_include(&mut set, include_override)?;
        set.validate_dependencies()?;
        return Ok(set);
    }

    if broker_only {
        return Ok(ComponentSet::from_slice(&[
            Component::Broker,
            Component::Controller,
            Component::Admin,
        ]));
    }
    if gateway_only {
        return Ok(ComponentSet::from_slice(&[
            Component::Gateway,
            Component::Admin,
        ]));
    }
    if dispatch_fleet {
        return Ok(ComponentSet::from_slice(&[Component::Dispatch]));
    }

    if let Some(spec) = &cfg.components {
        let mut set = if let Some(profile) = &spec.profile {
            ComponentProfile::parse(profile)?.components()
        } else if spec.include.is_empty() {
            ComponentSet::all()
        } else {
            ComponentSet::empty()
        };
        apply_include(&mut set, &spec.include)?;
        apply_include(&mut set, include_override)?;
        set.validate_dependencies()?;
        return Ok(set);
    }

    Ok(ComponentSet::all())
}

fn apply_include(set: &mut ComponentSet, include: &[String]) -> Result<(), ConfigError> {
    for item in include {
        set.insert(Component::parse(item)?);
    }
    Ok(())
}

pub fn resolve_panel_mode(
    cfg: &BetterMqConfig,
    panel_listen: Option<SocketAddr>,
    no_panel: bool,
    standalone_panel: bool,
) -> Result<PanelMode, ConfigError> {
    if standalone_panel {
        return Ok(PanelMode::Standalone);
    }
    if no_panel {
        return Ok(PanelMode::Disabled);
    }
    if panel_listen.is_some() {
        return Ok(PanelMode::SeparateListener);
    }
    if let Some(section) = &cfg.panel {
        if let Some(mode) = &section.mode {
            return PanelMode::parse(mode);
        }
        if section.listen.is_some() {
            return Ok(PanelMode::SeparateListener);
        }
    }
    Ok(PanelMode::Embedded)
}

pub fn resolve_listeners(
    public: SocketAddr,
    admin: Option<SocketAddr>,
    internal: Option<SocketAddr>,
    panel_listen: Option<SocketAddr>,
    panel_mode: PanelMode,
) -> ResolvedListeners {
    let admin = admin.or(panel_listen).unwrap_or(public);
    let internal = internal.unwrap_or(public);
    let collapsed =
        admin == public && internal == public && panel_mode != PanelMode::SeparateListener;
    let admin = if panel_mode == PanelMode::SeparateListener {
        panel_listen.or(Some(admin)).unwrap_or(public)
    } else {
        admin
    };
    ResolvedListeners {
        public,
        admin,
        internal,
        collapsed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_defaults_to_all_components() {
        let cfg = BetterMqConfig::template_single_local();
        let set = resolve_component_set(&cfg, None, &[], false, false, false, false).unwrap();
        assert!(set.contains(Component::Broker));
        assert!(set.contains(Component::Panel));
        assert!(set.contains(Component::Dispatch));
    }

    #[test]
    fn broker_only_alias_drops_local_dispatch() {
        let cfg = BetterMqConfig::template_single_local();
        let set = resolve_component_set(&cfg, None, &[], true, false, false, false).unwrap();
        assert!(set.contains(Component::Broker));
        assert!(!set.contains(Component::Dispatch));
        assert!(set.requires_local_storage());
    }

    #[test]
    fn gateway_and_panel_profiles_open_no_storage() {
        let cfg = BetterMqConfig::template_single_local();
        let gateway =
            resolve_component_set(&cfg, Some("gateway"), &[], false, false, false, false).unwrap();
        let panel = resolve_component_set(&cfg, None, &[], false, false, false, true).unwrap();
        assert!(!gateway.requires_local_storage());
        assert!(!panel.requires_local_storage());
        assert!(panel.contains(Component::Admin));
        assert!(panel.contains(Component::Panel));
    }

    #[test]
    fn panel_requires_admin() {
        let mut set = ComponentSet::from_slice(&[Component::Panel]);
        set.validate_dependencies().unwrap_err();
        set.insert(Component::Admin);
        set.validate_dependencies().unwrap();
    }
}
