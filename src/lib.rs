mod convert;
mod dialect;
mod fetcher;
mod loader;
use std::ops::{Deref, DerefMut};

use anyhow::{anyhow, Result};
pub use dialect::TyrDialect;
// use polars::prelude::Expr;
use polars::prelude::*;
use sqlparser::parser::Parser;

use crate::convert::Sql;
use crate::fetcher::retrieve_data;
use crate::loader::detect_content;

#[derive(Debug)]
pub struct DataSet(DataFrame);

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
}

pub async fn query<T: AsRef<str>>(sql: T) -> Result<DataSet> {
    let ast = Parser::parse_sql(&TyrDialect::default(), sql.as_ref())?;
    if ast.len() != 1 {
        return Err(anyhow!("Only support single sql at the moment"));
    };

    let sql = &ast[0];

    let Sql {
        source,
        condition,
        selection,
        offset,
        limit,
        order_by,
        group_by,
    } = sql.try_into()?;

    println!("selection: {:?}", selection);
    tracing::info!("retrieving data from source: {}", source);

    let ds = detect_content(retrieve_data(source).await?).load()?;

    let mut filtered = match condition {
        Some(expr) => ds.0.lazy().filter(expr),
        None => ds.0.lazy(),
    };

    println!("group_by: {:?}", group_by.to_vec());

    filtered = order_by.into_iter().fold(filtered, |acc, (col, desc)| {
        acc.sort(
            &col,
            SortOptions {
                descending: desc,
                nulls_last: true,
            },
        )
    });

    if offset.is_some() || limit.is_some() {
        filtered = filtered.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX) as u64);
    }

    Ok(DataSet(filtered.select(selection).collect()?))
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
