use clap::{App, Arg};
use config::Config;
use ingest_service::IngestService;

mod cache_service;
mod config;
mod database;
mod dedupe_command;
mod event;
mod ingest_service;
mod kafka_service;
mod utils;

use crate::cache_service::RedisCacheManager;
use crate::dedupe_command::DedupeCommand;
use crate::{database::Database, utils::setup_logger};

#[tokio::main]
async fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("event-type")
                .short("e")
                .long("event-type")
                .help("Type of event to dedupe")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("brokers list in kafka format")
                .takes_value(true)
                .default_value("KAFKA_CONFIG"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkfka=trace)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topics")
                .short("t")
                .long("topics")
                .help("Topic list")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log")
                .short("l")
                .long("log")
                .help("Print each consumed message."),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));
    let config = Config::new();

    // TODO: list topics
    let _topics = matches.values_of("topics").unwrap();
    // TODO: use passed in broker string instead of .env
    let _brokers = matches.value_of("brokers").unwrap();
    let _group_id = matches.value_of("group-id").unwrap();
    let event_type = matches.value_of("event-type").unwrap();
    let log = matches.is_present("log");

    let mut cache_manager = RedisCacheManager::new(&config.redis_connection_string());
    cache_manager.connect().await;

    let connection = Database::new(&config.db_connection_string()).await;
    let ingest_service = IngestService::new(connection, log);

    DedupeCommand::new(cache_manager, ingest_service, config, log)
        .execute(&event_type)
        .await;
}
