use anyhow::Result;
use queryer::query;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";

    let sql = format!(
        "SELECT location, last_updated_date, new_deaths \
        FROM {} where new_deaths >= 100 \
        ORDER BY new_cases DESC",
        url
    );

    let df1 = query(sql).await?;
    println!("{:#?}", df1);

    Ok(())
}
