use anyhow::Result;
use std::env;
use xq::{query, DataSet};

fn print_help() {
    println!("XQ - SQL Query Tool for Multiple Data Sources");
    println!("\nUsage:");
    println!("  xq <SQL_QUERY> [OPTIONS]");
    println!("  xq --help | -h");
    println!("\nOptions:");
    println!("  --format <FORMAT>    Output format: table (default), json, csv");
    println!("  --help, -h           Show this help message");
    println!("\nExamples:");
    println!("  xq \"SELECT * FROM https://example.com/data.csv WHERE value > 100\"");
    println!("  xq \"SELECT name, COUNT(*) FROM file:///path/to/data.csv GROUP BY name\"");
    println!("  xq \"SELECT * FROM file:///data.csv\" --format json");
    println!("\nSupported Data Sources:");
    println!("  - HTTP/HTTPS URLs (CSV/JSON format)");
    println!("  - Local files with file:// protocol (CSV/JSON format)");
    println!("\nSupported SQL Features:");
    println!("  - SELECT with column selection or *");
    println!("  - WHERE conditions");
    println!("  - GROUP BY");
    println!("  - Aggregation functions: SUM, COUNT, MAX, MIN, AVG");
    println!("  - ORDER BY (ASC/DESC)");
    println!("  - LIMIT and OFFSET");
}

fn format_dataframe(df: &DataSet, format: &str) -> String {
    match format {
        "json" => df
            .to_json()
            .unwrap_or_else(|e| format!("Error formatting JSON: {}", e)),
        "csv" => {
            let mut df_clone = DataSet(df.0.clone());
            df_clone
                .to_csv()
                .unwrap_or_else(|e| format!("Error formatting CSV: {}", e))
        }
        _ => df.to_table().to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Error: No SQL query provided");
        println!();
        print_help();
        std::process::exit(1);
    }

    let arg = &args[1];
    if arg == "--help" || arg == "-h" {
        print_help();
        return Ok(());
    }

    let sql = arg;

    // Parse format option
    let format = if args.len() > 2 && args[2] == "--format" && args.len() > 3 {
        &args[3]
    } else {
        "table"
    };

    match query(sql).await {
        Ok(df) => {
            println!("{}", format_dataframe(&df, format));
        }
        Err(e) => {
            eprintln!("Error executing query: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
