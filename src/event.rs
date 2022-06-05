use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
struct Game {
    Id: u32,
    Tags: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Mod {
    Id: u32,
    CreatorIds: Vec<u32>,
    Tags: Vec<String>,
    CreatedAtUtc: String,
    UpdatedAtUtc: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct Geo {
    UserIp: String,
    CountryCode: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct DeduplicationRules {
    HasBeenDeduplicated: bool,
    Period: String,
    Fields: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Event {
    EventId: String,
    EventTimestampUtc: String,
    EventType: String,
    Platform: String,
    Game: Game,
    Mod: Mod,
    Geo: Geo,
    Value: u32,
    DeduplicationRules: DeduplicationRules,
}
