use anyhow::{anyhow, Result};
use polars::prelude::*;
use std::io::Cursor;
use std::num::NonZero;

use crate::DataSet;

pub trait Load {
    type Error;
    fn load(self) -> Result<DataSet, Self::Error>;
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Loader {
    Csv(CsvLoader),
    Json(JsonLoader),
}

#[derive(Default, Debug)]
pub struct CsvLoader(pub(crate) String);

#[derive(Default, Debug)]
pub struct JsonLoader(pub(crate) String);

impl Loader {
    pub fn load(self) -> Result<DataSet> {
        match self {
            Loader::Csv(csv) => csv.load(),
            Loader::Json(json) => json.load(),
        }
    }
}

pub fn detect_content(data: String) -> Loader {
    // Try to detect JSON by checking if it starts with { or [
    let trimmed = data.trim();
    if (trimmed.starts_with('[') && trimmed.ends_with(']'))
        || (trimmed.starts_with('{') && trimmed.ends_with('}'))
    {
        Loader::Json(JsonLoader(data))
    } else {
        Loader::Csv(CsvLoader(data))
    }
}

impl Load for CsvLoader {
    type Error = anyhow::Error;

    fn load(self) -> Result<DataSet, Self::Error> {
        let cursor = Cursor::new(self.0.as_bytes());
        let df = CsvReadOptions::default()
            .with_infer_schema_length(Some(16))
            .into_reader_with_file_handle(cursor)
            .finish()?;

        Ok(DataSet(df))
    }
}

impl Load for JsonLoader {
    type Error = anyhow::Error;

    fn load(self) -> Result<DataSet, Self::Error> {
        let cursor = Cursor::new(self.0.as_bytes());
        let df = JsonReader::new(cursor)
            .infer_schema_len(NonZero::new(100))
            .finish()
            .map_err(|e| anyhow!("Failed to parse JSON: {}", e))?;

        Ok(DataSet(df))
    }
}
