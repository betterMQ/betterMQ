//! Shared curl-style HTTP fields on enqueue / publish / cron APIs.

use broker_partition::HttpDeliveryInput;
use std::collections::HashMap;

#[derive(Debug, serde::Deserialize, Default)]
pub struct OutboundHttpFields {
    #[serde(default)]
    pub method: Option<String>,
    #[serde(
        default,
        deserialize_with = "broker_partition::deserialize_optional_headers"
    )]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub sign: Option<bool>,
    #[serde(default)]
    pub request: Option<HttpDeliveryInput>,
}

impl OutboundHttpFields {
    pub fn apply_to(&self, req: &mut broker_partition::PublishRequest) {
        if self.method.is_some() {
            req.method = self.method.clone();
        }
        if self.headers.is_some() {
            req.headers = self.headers.clone();
        }
        if self.sign.is_some() {
            req.sign = self.sign;
        }
        if self.request.is_some() {
            req.request = self.request.clone();
        }
    }

    pub fn apply_to_scheduled(&self, req: &mut broker_schedule::ScheduledPublishRequest) {
        if self.method.is_some() {
            req.method = self.method.clone();
        }
        if self.headers.is_some() {
            req.headers = self.headers.clone();
        }
        if self.sign.is_some() {
            req.sign = self.sign;
        }
        if self.request.is_some() {
            req.request = self.request.clone();
        }
    }
}
