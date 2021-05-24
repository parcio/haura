//! Default `env_logger` configuration.

/// Initialise `env_logger` with a modified configuration.
/// This function exists for logging format consistency between multiple embedding
/// applications/libraries.
pub fn init_env_logger() {
    env_logger::Builder::from_default_env()
        .format_timestamp_millis()
        .init()
}
