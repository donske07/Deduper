use crate::{database::Database, event::Event};
use thiserror::Error;
use tokio_postgres::Row;

use serde_json;

pub struct IngestService {
    database: Database,
    log: bool,
}

impl IngestService {
    pub fn new(database: Database, log: bool) -> Self {
        Self { database, log }
    }

    async fn query(&self, query: &str) -> Result<Vec<Row>, DatabaseError> {
        match self.database.get_client().query(query, &[]).await {
            Ok(rows) => Ok(rows),
            _ => Err(DatabaseError::QueryError),
        }
    }

    pub fn build_raw_table_insert_statement<'a>(
        &self,
        query: &'a mut String,
        event_string: &str,
        is_last: bool,
    ) -> &'a mut String {
        *query += "(JSON_PARSE";
        *query += "('";
        *query += event_string;
        *query += "')";
        *query += if is_last { ")" } else { ")," };
        query
    }

    pub async fn insert(&self, events: Vec<Event>, event_type: &str) -> Result<(), DatabaseError> {
        match self.insert_into_raw_event_table(&events, &event_type).await {
            Ok(()) => {
                match self.insert_into_ingest_table(&events, &event_type).await {
                    Ok(()) => {
                        unimplemented!()
                    }
                    _ => println!("Error inserting events: {:?}", &events),
                };
            }
            _ => println!("Error inserting events: {:?}", &events),
        }
        Ok(())
    }

    pub async fn insert_into_ingest_table(
        &self,
        events: &Vec<Event>,
        event_type: &str,
    ) -> Result<(), DatabaseError> {
        let mut query = "INSERT INTO ".to_string() + event_type + " VALUES";
        for (index, event) in events.iter().enumerate() {
            let last_index = events.len() - 1;
            self.build_ingest_table_insert_statement(&mut query, event, index == last_index);
        }
        Ok(())
    }

    // private buildIngestInsertSqlQuery(batchId: string, events: Event[]) {
    // 	const length = events.length;

    // 	const insertValues = events.reduce((prev, event, index) => {
    // 		let insert: string = prev;
    // 		insert += '(';
    // 		insert += `'${event.EventId}', '${batchId}','${event.EventType}','${event.EventTimestampUtc}', '${
    // 			event.Platform
    // 		}',  null, ${event.Game.Id}, ${event.Mod.Id}, '${event.Geo.CountryCode}',null, false, ${this.getDateId(
    // 			event.EventTimestampUtc,
    // 		)}, ${event.Value}`;

    // 		insert += length - 1 === index ? ')' : '),';
    // 		return insert;
    // 	}, '');

    // 	return `INSERT INTO ${this.getIngestTableName()} VALUES ${insertValues};`;
    // }
    pub fn build_ingest_table_insert_statement<'a>(
        &self,
        query: &'a mut String,
        event: &Event,
        _is_last: bool,
    ) -> &'a mut String {
        println!("{:?}", event);
        query
    }

    async fn insert_into_raw_event_table(
        &self,
        events: &Vec<Event>,
        event_type: &str,
    ) -> Result<(), DatabaseError> {
        Ok(())
        // let mut query = "INSERT INTO ".to_string() + event_type + " VALUES";
        // for (index, event) in events.iter().enumerate() {
        //     match serde_json::to_string(&event) {
        //         Ok(event_string) => {
        //             let last_index = events.len() - 1;
        //             self.build_raw_table_insert_statement(
        //                 &mut query,
        //                 &event_string,
        //                 index == last_index,
        //             );
        //         }
        //         _ => {}
        //     }
        // }
        // query += ";";
        // if self.log {
        //     println!("Executing raw insert statement: {}", &query)
        // }
        // self.query(&query).await?;
        // Ok(())
    }
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Connection error")]
    QueryError,
}
