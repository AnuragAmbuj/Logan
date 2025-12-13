//! The logan server library

#![deny(unreachable_pub)]

pub mod error;
pub mod offset_manager;
pub mod server;
pub mod shard;

mod dispatch;

pub use error::ServerError;
pub use server::Server;

pub type Result<T> = std::result::Result<T, error::ServerError>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
