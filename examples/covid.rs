use anyhow::Result;
use xq::query;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    //
    // let sql1 = format!(
    //     "SELECT location, last_updated_date, new_deaths \
    //     FROM {} where new_deaths >= 100 \
    //     ORDER BY new_cases DESC",
    //     url
    // );
    //
    // println!("{:#?}", query(sql1).await?);

    let sql2 = format!(
        "SELECT location, sum(new_deaths) \
        FROM {} where new_deaths >= 100 \
        GROUP BY location",
        url
    );
    println!("{:#?}", query(sql2).await?);

    Ok(())
}
