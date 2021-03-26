pub fn init_env_logger() {
    env_logger::Builder::from_default_env()
        .format_timestamp_millis()
        .init()
}
