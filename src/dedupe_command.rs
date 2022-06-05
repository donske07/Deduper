use std::{sync::Arc, time::Duration};

use crate::cache_service::RedisCacheManager;
use crate::event::Event;
use crate::ingest_service::IngestService;
use crate::utils::Batch;
use futures::StreamExt;
use log::warn;
use rskafka::client::{
    consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
    ClientBuilder,
};
use tokio::time::timeout;

use serde_json::Value;

const BATCH_LIMIT: u8 = 10;
const BATCH_TIMEOUT: u64 = 500;

pub struct DedupeCommand {
    cache_store: RedisCacheManager,
    ingest_service: IngestService,
}

impl DedupeCommand {
    pub fn new(cache_manager: RedisCacheManager, ingest_service: IngestService, log: bool) -> Self {
        Self {
            cache_store: cache_manager,
            ingest_service,
        }
    }

    pub async fn execute(&self, brokers: &str, group_id: &str, event_type: &str) {
        println!("running deduper");
        self.consume(brokers, group_id, event_type).await;
    }

    pub async fn consume(&self, brokers: &str, group_id: &str, event_type: &str) {
        let connection = "KAFKA_CONFIG".to_owned();
        let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
        let partition_client = Arc::new(
            client
                .partition_client(event_type.to_string(), 0)
                .await
                .unwrap(),
        );

        let mut stream = StreamConsumerBuilder::new(partition_client, StartOffset::Latest)
            .with_max_wait_ms(100)
            .build();

        loop {
            let mut batch = Batch::<Event>::new();
            timeout(
                Duration::from_millis(BATCH_TIMEOUT),
                self.fill_bag(&mut stream, &mut batch, BATCH_LIMIT),
            )
            .await
            .ok();

            self.ingest_service
                .insert(batch.get_bag(), &event_type)
                .await
                .ok();
        }
    }

    async fn fill_bag(&self, consumer: &mut StreamConsumer, batch: &mut Batch<Event>, limit: u8) {
        while batch.count() < limit {
            match consumer.next().await {
                Some(Err(e)) => warn!("Kafka error: {}", e),
                Some(Ok((record, _high_water_mark))) => {
                    let record = String::from_utf8(record.record.value.unwrap()).unwrap();
                    let event: Event = serde_json::from_str(&record).unwrap();
                    let value: Value = serde_json::from_str(&record).unwrap();
                    let dedupe_cache_key = self.generate_dedupe_cache_key(&value);
                    let event_id = value["EventId"].as_str().unwrap().to_string().clone();
                    if self.cache_store.has(&event_id, &dedupe_cache_key).await == false {
                        println!("save to redshift {:?}", &dedupe_cache_key);
                        self.cache_store.remember(&dedupe_cache_key).await.unwrap();
                        self.cache_store.remember(&event_id).await.unwrap();
                        batch.add(event);
                    };
                }
                _ => warn!("error"),
            }
        }
    }

    fn generate_dedupe_cache_key(&self, value: &Value) -> String {
        let mut dedupe_cache_key = String::from("");
        let fields = value["DeduplicationRules"]["Fields"].as_array().unwrap();
        for field in fields {
            dedupe_cache_key.push_str(field.as_str().unwrap());
            let parts = field.as_str().unwrap().split(".");
            let mut field_value = value;
            for part in parts.into_iter() {
                field_value = &field_value[part];
            }
            match field_value.as_str() {
                Some(value) => {
                    dedupe_cache_key.push_str(value);
                }
                None => {
                    dedupe_cache_key.push_str(
                        u64::from(field_value.as_u64().unwrap())
                            .to_string()
                            .as_str(),
                    );
                }
            }
        }
        dedupe_cache_key
    }
}
