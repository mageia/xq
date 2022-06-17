use sqlparser::{dialect::GenericDialect, parser::Parser};

fn main() {
    tracing_subscriber::fmt::init();

    // let sql = "SELECT a a1, myfunc(b), * \
    //     FROM data_source \
    //     WHERE a > b AND b < 100 AND c BETWEEN 10 AND 20 \
    //     ORDER BY a DESC, b \
    //     LIMIT 10 OFFSET 10";

    let sql = "select a, b, sum(c) from x group by a, b";
    let ast = Parser::parse_sql(&GenericDialect::default(), sql);
    println!("{:#?}", ast);
}
