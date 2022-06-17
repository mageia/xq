use queryer::query;
use std::env;

#[tokio::main]
async fn main() {
    let sql = env::args().nth(1).unwrap();
    // let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";

    // let sql = format!(
    //     "SELECT location , total_cases, new_cases, total_deaths, new_deaths \
    //     FROM {} where new_deaths >= 1000 ORDER BY new_cases DESC",
    //     url
    // );

    // let df1 = query(sql).await.unwrap();
    // if let Ok(df) = query(sql).await {
    //     println!("{:#?}", df)
    // } else {
    //     println!("{:#?}", "Failed")
    // }

    match query(sql).await {
        Ok(df) => println!("{:#?}", df),
        Err(e) => println!("{:#?}", e),
    }
}
