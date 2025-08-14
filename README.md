# Logan - A Kafka-compatible Message Broker in Rust
## EARLY STAGE WIP, NOT READY FOR USAGE

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://github.com/AnuragAmbuj/logan/actions/workflows/rust.yml/badge.svg)](https://github.com/yourusername/logan/actions)
[![Documentation](https://docs.rs/logan/badge.svg)](https://docs.rs/logan)

Logan is a high-performance, Kafka-compatible message broker implemented in Rust, focusing on efficiency, reliability, and ease of use.

## Project Structure

The project is organized as a Cargo workspace with the following crates:

- `logan-bin`: Binary entry point for the broker
- `logan-protocol`: Core protocol definitions and serialization/deserialization
- `logan-storage`: Persistent storage engine for messages and metadata
- `logan-server`: Network server implementation
- `logan-client`: Client library for interacting with the broker
- `logan-common`: Shared utilities and types

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo

### Building

```bash
# Build all crates
cargo build --release

# Build and run the broker
cargo run -p logan-bin
```

## Development

### Adding a New Crate

To add a new crate to the workspace:

1. Create a new directory in the workspace root
2. Initialize a new library crate: `cargo init --lib`
3. Add the crate to the workspace members in `Cargo.toml`

### Running Tests

```bash
# Run all tests
cargo test --workspace

# Test a specific crate
cargo test -p logan-protocol
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
