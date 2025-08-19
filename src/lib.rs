mod convert;
mod dialect;
mod fetcher;
mod loader;
use std::ops::{Deref, DerefMut};

use anyhow::{anyhow, Result};
pub use dialect::XQDialect;
use polars::prelude::*;
use prettytable::{Cell, Row, Table};
use sqlparser::parser::Parser;

use crate::convert::Sql;
use crate::fetcher::retrieve_data;
use crate::loader::detect_content;

#[derive(Debug, Clone)]
pub struct DataSet(pub DataFrame);

impl Deref for DataSet {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DataSet {
    pub fn to_csv(&mut self) -> Result<String> {
        let mut buf = Vec::new();
        let mut writer = CsvWriter::new(&mut buf);
        writer.finish(self)?;
        Ok(String::from_utf8(buf)?)
    }

    pub fn to_table(&self) -> Table {
        let mut table = Table::new();

        // Add header
        let headers: Vec<_> = self
            .0
            .get_column_names()
            .into_iter()
            .map(Cell::new)
            .collect();
        table.add_row(Row::new(headers));

        // Add data rows
        let height = self.0.height();
        let width = self.0.width();

        for row_idx in 0..height.min(1000) {
            // Limit to 1000 rows for display
            let mut row_cells = Vec::new();
            for col_idx in 0..width {
                let series = &self.0.get_columns()[col_idx];
                let value = format!("{}", series.get(row_idx).unwrap());
                row_cells.push(Cell::new(&value));
            }
            table.add_row(Row::new(row_cells));
        }

        if height > 1000 {
            let mut footer = Vec::new();
            for _ in 0..width {
                footer.push(Cell::new("..."));
            }
            table.add_row(Row::new(footer));
        }

        table
    }

    pub fn to_json(&self) -> Result<String> {
        let records = self
            .0
            .iter()
            .map(|row| {
                let mut record = serde_json::Map::new();
                for (i, value) in row.iter().enumerate() {
                    let col_name = self.0.get_column_names()[i];
                    let json_value = match value {
                        AnyValue::Null => serde_json::Value::Null,
                        AnyValue::Boolean(b) => serde_json::Value::Bool(b),
                        AnyValue::Int8(n) => serde_json::Value::Number(n.into()),
                        AnyValue::Int16(n) => serde_json::Value::Number(n.into()),
                        AnyValue::Int32(n) => serde_json::Value::Number(n.into()),
                        AnyValue::Int64(n) => serde_json::Value::Number(n.into()),
                        AnyValue::UInt8(n) => serde_json::Value::Number(n.into()),
                        AnyValue::UInt16(n) => serde_json::Value::Number(n.into()),
                        AnyValue::UInt32(n) => serde_json::Value::Number(n.into()),
                        AnyValue::UInt64(n) => serde_json::Value::Number(n.into()),
                        AnyValue::Float32(n) => serde_json::json!(n),
                        AnyValue::Float64(n) => serde_json::json!(n),
                        AnyValue::String(s) => serde_json::Value::String(s.to_string()),
                        AnyValue::StringOwned(ref s) => serde_json::Value::String(s.to_string()),
                        _ => serde_json::Value::String(format!("{}", value)),
                    };
                    record.insert(col_name.to_string(), json_value);
                }
                serde_json::Value::Object(record)
            })
            .collect::<Vec<_>>();

        Ok(serde_json::to_string_pretty(&records)?)
    }
}

pub async fn query<T: AsRef<str>>(sql: T) -> Result<DataSet> {
    let ast = Parser::parse_sql(&XQDialect, sql.as_ref())?;
    if ast.len() != 1 {
        return Err(anyhow!("Only support single sql at the moment"));
    };

    let sql = &ast[0];

    let Sql {
        selection,
        source,
        condition,
        group_by,
        aggregation,
        offset,
        limit,
        order_by,
    } = sql.try_into()?;

    tracing::debug!("retrieving data from source: {}", source);

    let ds = detect_content(retrieve_data(source).await?).load()?;

    // println!("group_by: {:?}", group_by.to_vec());
    // println!("selection: {:?}", selection);

    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };

    // println!("aggregation: {:?}", aggregation.to_vec());

    if !aggregation.is_empty() {
        if !group_by.is_empty() {
            filtered = filtered.group_by(group_by).agg(aggregation);
        } else {
            // When we have aggregation but no group by, aggregate the entire dataset
            filtered = filtered.select(aggregation);
        }
    }

    if !order_by.is_empty() {
        let cols: Vec<_> = order_by.iter().map(|(col, _)| col.clone()).collect();
        let descending: Vec<_> = order_by.iter().map(|(_, desc)| *desc).collect();
        filtered = filtered.sort(
            cols,
            SortMultipleOptions {
                descending,
                nulls_last: vec![true; order_by.len()],
                multithreaded: true,
                maintain_order: false,
            },
        );
    }

    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX) as u32);
    }

    Ok(DataSet(filtered.select(selection).collect()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_csv_query() {
        let csv_data = "name,age,score\nAlice,20,85\nBob,22,90\nCharlie,21,88";
        let temp_file = std::env::temp_dir().join("test.csv");
        std::fs::write(&temp_file, csv_data).unwrap();

        let sql = format!("SELECT * FROM file://{}", temp_file.display());
        let result = query(&sql).await;
        assert!(result.is_ok());

        let df = result.unwrap();
        assert_eq!(df.0.height(), 3);
        assert_eq!(df.0.width(), 3);

        std::fs::remove_file(temp_file).ok();
    }

    #[tokio::test]
    async fn test_json_query() {
        let json_data = r#"[
            {"name": "Alice", "age": 20, "score": 85},
            {"name": "Bob", "age": 22, "score": 90}
        ]"#;
        let temp_file = std::env::temp_dir().join("test.json");
        std::fs::write(&temp_file, json_data).unwrap();

        let sql = format!(
            "SELECT name, score FROM file://{} WHERE age > 20",
            temp_file.display()
        );
        let result = query(&sql).await;
        assert!(result.is_ok());

        let df = result.unwrap();
        assert_eq!(df.0.height(), 1); // Only Bob matches the condition

        std::fs::remove_file(temp_file).ok();
    }

    #[tokio::test]
    async fn test_aggregation_query() {
        let csv_data = "category,value\nA,10\nB,20\nA,15\nB,25\nA,5";
        let temp_file = std::env::temp_dir().join("test_agg.csv");
        std::fs::write(&temp_file, csv_data).unwrap();

        let sql = format!(
            "SELECT category, SUM(value), COUNT(*) FROM file://{} GROUP BY category",
            temp_file.display()
        );
        let result = query(&sql).await;
        if let Err(e) = &result {
            eprintln!("Test error: {:?}", e);
        }
        assert!(result.is_ok());

        let df = result.unwrap();
        assert_eq!(df.0.height(), 2); // Two categories

        std::fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_dataset_to_csv() {
        use polars::df;
        let df = df! {
            "name" => &["Alice", "Bob"],
            "age" => &[20, 22],
        }
        .unwrap();

        let mut dataset = DataSet(df);
        let csv_result = dataset.to_csv();
        assert!(csv_result.is_ok());

        let csv = csv_result.unwrap();
        assert!(csv.contains("name,age"));
        assert!(csv.contains("Alice,20"));
        assert!(csv.contains("Bob,22"));
    }
}
