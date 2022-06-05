use dotenv::dotenv;
use std::env;

pub struct Config {
    db_host: String,
    db_port: String,
    db_user: String,
    db_password: String,
    db_table: String,

    redis_host: String,
    redis_port: String,

    kafka_host: String,
    kafka_port: String,
}

impl Config {
    pub fn new() -> Self {
        dotenv().ok();
        return Self {
            db_host: dotenv::var("db_host").unwrap(),
            db_port: dotenv::var("db_port").unwrap(),
            db_user: dotenv::var("db_user").unwrap(),
            db_password: dotenv::var("db_password").unwrap(),
            db_table: dotenv::var("db_table").unwrap(),
            redis_host: dotenv::var("redis_host").unwrap(),
            redis_port: dotenv::var("redis_port").unwrap(),
            kafka_host: dotenv::var("kafka_host").unwrap(),
            kafka_port: dotenv::var("kafka_port").unwrap(),
        };
    }

    pub fn db_connection_string(&self) -> String {
        let host = "host=".to_string() + self.db_host.as_str();
        let port = "port=".to_string() + self.db_port.as_str();
        let user = "user=".to_string() + self.db_user.as_str();
        let password = "password=".to_string() + self.db_password.as_str();
        let db_name = "dbname=".to_string() + self.db_table.as_str();

        return host
            + " "
            + port.as_str()
            + " "
            + user.as_str()
            + " "
            + password.as_str()
            + " "
            + db_name.as_str();
    }

    pub fn redis_connection_string(&self) -> String {
        return "redis://".to_string() + self.redis_host.as_str() + ":" + self.redis_port.as_str();
    }

    pub fn kafka_connection_string(&self) -> String {
        return self.kafka_host.to_string() + ":" + self.kafka_port.as_str();
    }
}
