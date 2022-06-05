use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client, RedisError};
use tokio::join;

use thiserror::Error;
pub struct RedisCacheManager {
    client: Client,
    connection: Option<MultiplexedConnection>,
}

impl RedisCacheManager {
    pub fn new(url: &str) -> Self {
        let client = match Client::open(url) {
            Ok(client) => client,
            _ => panic!("Unable to connect to redis."),
        };

        Self {
            client,
            connection: None,
        }
    }

    pub async fn connect(&mut self) {
        let connection = self
            .client
            .get_multiplexed_tokio_connection()
            .await
            .expect("Unable to connect to Redis.");
        self.connection = Some(connection);
    }
}

impl RedisCacheManager {
    fn _forget(&self, _key: &str) {}

    pub async fn has(&self, event_id: &str, dedupe_cache_key: &str) -> bool {
        match &self.connection {
            Some(_) => {
                return match join!(self.get(event_id), self.get(dedupe_cache_key)) {
                    (Err(_), Ok(_)) | (Ok(_), Err(_)) => true,
                    _ => false,
                };
            }
            _ => panic!("No redis connection."),
        }
    }

    pub async fn get(&self, key: &str) -> Result<u32, CacheError> {
        match &self.connection {
            Some(connection) => {
                let mut conn = connection.clone();
                conn.get(key).await.map_err(|_| CacheError::RedisError)
            }
            _ => Err(CacheError::RedisError),
        }
    }

    pub async fn remember(&self, key: &str) -> Result<(), CacheError> {
        match &self.connection {
            Some(connection) => {
                let mut conn = connection.clone();
                conn.set::<&str, u32, ()>(key, 1).await?;
                Ok(())
            }
            _ => Err(CacheError::RedisError),
        }
    }
}

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("redis error")]
    RedisError,
}

impl From<RedisError> for CacheError {
    fn from(_e: RedisError) -> Self {
        CacheError::RedisError
    }
}
