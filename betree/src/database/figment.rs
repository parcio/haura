use figment::{
    providers::{Env, Serialized},
    Provider,
};

impl super::DatabaseConfiguration {
    /// A figment provider for the default DatabaseConfiguration.
    pub fn figment_default() -> impl Provider {
        Serialized::defaults(super::DatabaseConfiguration::default())
    }

    /// Construct a default configuration provider, using environment variables only.
    /// E.g. BETREE__FOO__BAR maps to foo.bar in the configuration
    pub fn figment_env() -> impl Provider {
        Env::prefixed("BETREE__").ignore(&["CONFIG"]).split("__")
    }
}
