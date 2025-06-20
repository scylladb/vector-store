pub struct Info;

impl Info {
    pub fn name() -> &'static str {
        env!("CARGO_PKG_NAME")
    }

    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
