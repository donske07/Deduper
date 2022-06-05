use tokio_postgres::{Client, Connection, Error, NoTls, Row, Socket};
pub struct Database {
    client: Client,
}

impl Database {
    pub async fn new(config: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(config, NoTls)
            .await
            .expect("could not connect to database");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                println!("connection error: {}", e);
            }
        });

        Self { client }
    }

    pub fn get_client(&self) -> &Client {
        &self.client
    }
}
