fn main() {
    println!("Hello, world!");
}

use std::sync::OnceLock;

pub fn cached_hostname() -> &'static str {
    static HOSTNAME: OnceLock<String> = OnceLock::new();

    HOSTNAME.get_or_init(|| {
        gethostname::gethostname().to_string_lossy().into_owned()
    })
}