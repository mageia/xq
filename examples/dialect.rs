use sqlparser::{dialect::GenericDialect, parser::Parser};

fn main() {
    tracing_subscriber::fmt::init();

    let sql1 = "SELECT a a1, myfunc(b), * \
        FROM data_source \
        WHERE a > b AND b < 100 AND c BETWEEN 10 AND 20 \
        ORDER BY a DESC, b \
        LIMIT 10 OFFSET 10";

    let ast1 = Parser::parse_sql(&GenericDialect, sql1);
    println!("sql1 = {:#?}", sql1);
    println!("ast1 = {:#?}", ast1);

    let sql2 = "select a, b, sum(c) from x group by a, b";
    let ast2 = Parser::parse_sql(&GenericDialect, sql2);
    println!("sql2 = {:#?}", sql2);
    println!("ast2 = {:#?}", ast2);
}
