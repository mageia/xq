use anyhow::Result;
use xq::query;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    //
    // let url1 = "file:///tmp/owid-covid-latest.csv";
    // let sql1 = format!(
    //     "SELECT last_updated_date, location, new_deaths \
    //     FROM {} where new_deaths >= 40",
    //     url1
    // );
    // println!("{:#?}", query(sql1).await?);
    //
    // let url2 = "file:///tmp/owid-covid-latest.csv";
    // let sql2 = format!(
    //     "select \
    //         last_updated_date as lud, location as l, sum(new_deaths) \
    //     from {} \
    //     where new_deaths >= 40 \
    //     group by last_updated_date, location \
    //     order by sum_new_deaths desc",
    //     url2
    // );
    // println!("{:?}", query(sql2).await?);

    let url3 = "file:///tmp/a.csv";
    let sql3 = format!(
        "SELECT a, b, c, v FROM {} where v >= 0 order by a, b, c",
        url3
    );
    println!("{:?}", query(sql3).await?);

    let url4 = "file:///tmp/a.csv";
    let sql4 = format!(
        "SELECT a, b, sum(v) FROM {} where v >= 0 group by a, b order by a, b",
        url4
    );
    println!("{:?}", query(sql4).await?);

    Ok(())
}
