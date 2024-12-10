use std::collections::HashMap;

use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowsResp {
    pub cursor: Option<String>,
    pub records: Vec<Follow>,
}
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Follow {
    pub uri: String,
    pub cid: String,
    pub value: FollowVal,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowVal {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub subject: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BskyEvent {
    pub did: String,
    #[serde(rename = "time_us")]
    pub time_us: i64,
    pub kind: String,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub commit: Option<Commit>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: Option<Record>,
    pub cid: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub created_at: String,
    pub subject: Option<Subj>,
    pub lang: Option<String>,
    pub langs: Option<Vec<String>>,
    pub facets: Option<Vec<Facet>>,
    pub text: Option<String>,
    pub reply: Option<Reply>,
    pub embed: Option<Embed>,
    pub images: Option<Vec<Image>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Facet {
    pub index: Option<Index>,
    pub features: Option<Vec<Feature>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub byte_start: u64,
    pub byte_end: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Feature {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub uri: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Image {
    pub alt: Option<String>,
    pub aspect_ratio: Option<HashMap<String, String>>,
    pub image: Option<ImageInternal>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageInternal {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    #[serde(rename = "ref")]
    pub reff: Ref,
    pub mime_type: String,
    pub size: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ref {
    #[serde(rename = "$link")]
    pub link: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Embed {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub uri: Option<String>,
    pub embedded: Option<Embd>,
    pub external: Option<External>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct External {
    pub title: Option<String>,
    pub uri: Option<String>,
    pub description: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reply {
    pub parent: Parent,
    pub root: Parent,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parent {
    pub cid: String,
    pub uri: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub cid: String,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Subj {
    T1(String),
    T2(Subject),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Embd {
    T1(String),
    T2(Subject),
}
