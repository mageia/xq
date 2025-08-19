use anyhow::Result;
use xq::query;

#[tokio::main]
async fn main() -> Result<()> {
    println!("XQ Demo - SQL Query for Multiple Data Sources\n");
    
    // Example 1: Query CSV file
    println!("Example 1: Query local CSV file");
    let csv_path = std::env::current_dir()?.join("examples/a.csv");
    let sql1 = format!(
        "SELECT a, b, SUM(v) FROM file://{} WHERE v > 0 GROUP BY a, b ORDER BY sum_v DESC",
        csv_path.display()
    );
    println!("SQL: {}", sql1);
    match query(&sql1).await {
        Ok(df) => println!("Result:\n{}", df.to_table()),
        Err(e) => println!("Error: {}", e),
    }
    println!("\n---\n");
    
    // Example 2: Query JSON file
    println!("Example 2: Query local JSON file");
    let json_path = std::env::current_dir()?.join("examples/data.json");
    let sql2 = format!(
        "SELECT city, COUNT(*) as count, AVG(salary) as avg_salary FROM file://{} GROUP BY city ORDER BY avg_salary DESC",
        json_path.display()
    );
    println!("SQL: {}", sql2);
    match query(&sql2).await {
        Ok(df) => println!("Result:\n{}", df.to_table()),
        Err(e) => println!("Error: {}", e),
    }
    println!("\n---\n");
    
    // Example 3: Query with WHERE and LIMIT
    println!("Example 3: Query with WHERE and LIMIT");
    let sql3 = format!(
        "SELECT name, age, salary FROM file://{} WHERE age > 30 ORDER BY salary DESC LIMIT 5",
        json_path.display()
    );
    println!("SQL: {}", sql3);
    match query(&sql3).await {
        Ok(df) => println!("Result:\n{}", df.to_table()),
        Err(e) => println!("Error: {}", e),
    }
    println!("\n---\n");
    
    // Example 4: Query from HTTP (commented out to avoid external dependency)
    println!("Example 4: Query from HTTP (example only)");
    println!("SQL: SELECT location, total_cases FROM https://example.com/covid-data.csv WHERE total_cases > 1000000 ORDER BY total_cases DESC LIMIT 10");
    println!("(Skipped - requires actual HTTP endpoint)\n");
    
    Ok(())
}