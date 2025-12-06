use tracing::Level;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

pub fn init_logging(level: Level) {
    let formatting_layer = fmt::layer()
        .with_ansi(true)
        .with_level(true)
        .with_timer(fmt::time::UtcTime::rfc_3339())
        .with_writer(std::io::stderr);

    let filter_layer = tracing_subscriber::filter::LevelFilter::from_level(level);

    tracing_subscriber::registry()
        .with(formatting_layer)
        .with(filter_layer)
        .init();
}
