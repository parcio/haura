use figment::{
    providers::{Env, Serialized},
    Provider,
};

impl super::DatabaseConfiguration {
    pub fn figment_default() -> impl Provider {
        Serialized::defaults(super::DatabaseConfiguration::default())
    }

    /// Construct a default configuration provider, using environment variables only.
    pub fn figment_env() -> impl Provider {
        Env::prefixed("BETREE__").ignore(&["CONFIG"]).split("__")
    }
}
