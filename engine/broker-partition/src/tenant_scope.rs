//! Request-scoped tenant id for multi-tenant cloud brokers.
//!
//! Self-host keeps `BrokerConfig.tenant_id` (`"default"`). Cloud auth sets the
//! authenticated tenant for the duration of each request via `scope_tenant`.

use std::borrow::Cow;

tokio::task_local! {
    static REQUEST_TENANT: String;
}

/// Active tenant for broker catalog / publish paths.
pub fn effective_tenant<'a>(config_tenant: &'a str) -> Cow<'a, str> {
    match REQUEST_TENANT.try_with(|t| t.clone()) {
        Ok(t) if !t.is_empty() => Cow::Owned(t),
        _ => Cow::Borrowed(config_tenant),
    }
}

/// Run `fut` with `tenant_id` bound for all nested broker calls.
pub async fn scope_tenant<F, R>(tenant_id: String, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    REQUEST_TENANT.scope(tenant_id, fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn falls_back_to_config_outside_scope() {
        assert_eq!(effective_tenant("default").as_ref(), "default");
        let got = scope_tenant("tenant-a".into(), async {
            effective_tenant("default").into_owned()
        })
        .await;
        assert_eq!(got, "tenant-a");
        assert_eq!(effective_tenant("default").as_ref(), "default");
    }
}
