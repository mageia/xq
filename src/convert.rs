use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
use polars::lazy::dsl::AggExpr;
use polars::prelude::{col, count, Expr, LiteralValue, Operator};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Offset as SqlOffset, OrderByExpr, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};

pub struct Sql<'a> {
    pub selection: Vec<Expr>,
    pub source: &'a str,
    pub condition: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub aggregation: Vec<Expr>,
    pub order_by: Vec<(String, bool)>,
    pub offset: Option<i64>,
    pub limit: Option<usize>,
}

pub struct Expression(Box<SqlExpr>);
pub struct Operation(SqlBinaryOperator);
pub struct Projection<'a>(&'a SelectItem);
pub struct Source<'a>(&'a [TableWithJoins]);
pub struct Order<'a>(&'a OrderByExpr);
pub struct Offset<'a>(&'a SqlOffset);
pub struct Limit<'a>(&'a SqlExpr);
pub struct Value(SqlValue);
pub struct GroupBy<'a>(&'a SqlExpr);

impl TryFrom<Expression> for Expr {
    type Error = anyhow::Error;

    fn try_from(expr: Expression) -> Result<Self, Self::Error> {
        match *expr.0 {
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryExpr {
                left: Box::new(Expression(left).try_into()?),
                op: Operation(op).try_into()?,
                right: Box::new(Expression(right).try_into()?),
            }),
            // SqlExpr::IsNull(expr) => Ok(Self::IsNull(Box::new(Expression(expr).try_into()?))),
            // SqlExpr::IsNotNull(expr) => Ok(Self::IsNotNull(Box::new(Expression(expr).try_into()?))),
            SqlExpr::Identifier(id) => Ok(Self::Column(Arc::from(id.value))),
            SqlExpr::Value(v) => Ok(Self::Literal(Value(v).try_into()?)),
            v => Err(anyhow!("expr {:#?} is not supported", v)),
        }
    }
}

impl TryFrom<Operation> for Operator {
    type Error = anyhow::Error;

    fn try_from(op: Operation) -> Result<Self, Self::Error> {
        match op.0 {
            SqlBinaryOperator::Plus => Ok(Self::Plus),
            SqlBinaryOperator::Minus => Ok(Self::Minus),
            SqlBinaryOperator::Multiply => Ok(Self::Multiply),
            SqlBinaryOperator::Divide => Ok(Self::Divide),
            SqlBinaryOperator::Modulo => Ok(Self::Modulus),
            SqlBinaryOperator::Gt => Ok(Self::Gt),
            SqlBinaryOperator::Lt => Ok(Self::Lt),
            SqlBinaryOperator::GtEq => Ok(Self::GtEq),
            SqlBinaryOperator::LtEq => Ok(Self::LtEq),
            SqlBinaryOperator::Eq => Ok(Self::Eq),
            SqlBinaryOperator::NotEq => Ok(Self::NotEq),
            SqlBinaryOperator::And => Ok(Self::And),
            SqlBinaryOperator::Or => Ok(Self::Or),
            v => Err(anyhow!("Operator {} is not supported", v)),
        }
    }
}

impl<'a> TryFrom<GroupBy<'a>> for Expr {
    type Error = anyhow::Error;

    fn try_from(gb: GroupBy<'a>) -> Result<Self, Self::Error> {
        match gb.0 {
            SqlExpr::Identifier(id) => Ok(col(&id.value)),
            v => Err(anyhow!("expr {:#?} is not supported", v)),
        }
    }
}

impl<'a> TryFrom<Projection<'a>> for Expr {
    type Error = anyhow::Error;

    fn try_from(p: Projection<'a>) -> Result<Self, Self::Error> {
        // println!("{:#?}", p.0);

        match p.0 {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => Ok(col(&id.to_string())),
            SelectItem::QualifiedWildcard(v, _) => Ok(col(&v.to_string())),
            SelectItem::Wildcard(_) => Ok(col("*")),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(id),
                alias,
            } => Ok(Expr::Alias(
                Box::new(Expr::Column(Arc::from(id.to_string()))),
                Arc::from(alias.to_string()),
            )),
            SelectItem::UnnamedExpr(SqlExpr::Function(f)) => match f.name.to_string().as_str() {
                "count" => Ok(col(&f.args[0].to_string()).count()),
                "sum" => Ok(col(&f.args[0].to_string()).sum()),
                // "max" => Ok(col(&f.args[0].to_string()).max()),
                // "min" => Ok(col(&f.args[0].to_string()).min()),
                // "mean" | "avg" => Ok(col(&f.args[0].to_string()).mean()),
                unknown => Err(anyhow!("function {} not support yet", unknown)),
            },
            item => Err(anyhow!("Projection {} not supported", item)),
        }
    }
}

impl<'a> TryFrom<Source<'a>> for &'a str {
    type Error = anyhow::Error;

    fn try_from(source: Source<'a>) -> Result<Self, Self::Error> {
        if source.0.len() != 1 {
            return Err(anyhow!("We only support single data source at the moment"));
        }

        let table = &source.0[0];
        if !table.joins.is_empty() {
            return Err(anyhow!("We do not support join data source at the moment"));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => Ok(&name.0.first().unwrap().value),
            _ => Err(anyhow!("We only support table")),
        }
    }
}

impl<'a> TryFrom<Order<'a>> for (String, bool) {
    type Error = anyhow::Error;

    fn try_from(o: Order<'a>) -> Result<Self, Self::Error> {
        let name = match &o.0.expr {
            SqlExpr::Identifier(id) => id.to_string(),
            expr => {
                return Err(anyhow!(
                    "We only support Identifier for order by, got {}",
                    expr
                ))
            }
        };
        Ok((name, !o.0.asc.unwrap_or(true)))
    }
}

impl<'a> From<Offset<'a>> for i64 {
    fn from(offset: Offset<'a>) -> Self {
        match offset.0 {
            SqlOffset {
                value: SqlExpr::Value(SqlValue::Number(v, _b)),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

impl<'a> From<Limit<'a>> for usize {
    fn from(l: Limit<'a>) -> Self {
        match l.0 {
            SqlExpr::Value(SqlValue::Number(v, _b)) => v.parse().unwrap_or(usize::MAX),
            _ => usize::MAX,
        }
    }
}

impl TryFrom<Value> for LiteralValue {
    type Error = anyhow::Error;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.0 {
            SqlValue::Number(v, _) => Ok(LiteralValue::Float64(v.parse().unwrap())),
            SqlValue::Boolean(v) => Ok(LiteralValue::Boolean(v)),
            SqlValue::Null => Ok(LiteralValue::Null),
            SqlValue::SingleQuotedString(v) => Ok(LiteralValue::Utf8(v)),
            SqlValue::DoubleQuotedString(v) => Ok(LiteralValue::Utf8(v)),
            v => Err(anyhow!("Value {} is not support", v)),
        }
    }
}

impl<'a> TryFrom<&'a Statement> for Sql<'a> {
    type Error = anyhow::Error;

    fn try_from(sql: &'a Statement) -> Result<Self, Self::Error> {
        match sql {
            Statement::Query(q) => {
                let offset = q.offset.as_ref();
                let limit = q.limit.as_ref();
                let orders = &q.order_by;
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    projection,
                    group_by: group_by_clause,
                    ..
                } = match &q.body.as_ref() {
                    SetExpr::Select(statement) => statement.as_ref(),
                    _ => return Err(anyhow!("Only support `Select` Query now")),
                };

                let source = Source(table_with_joins).try_into()?;
                let condition = match where_clause {
                    Some(expr) => Some(Expression(Box::new(expr.to_owned())).try_into()?),
                    None => None,
                };

                let mut group_by = Vec::with_capacity(8);
                for g in group_by_clause {
                    group_by.push(GroupBy(g).try_into()?);
                }

                let mut selection = Vec::with_capacity(8);
                let mut aggregation = Vec::with_capacity(8);

                for p in projection {
                    let expr = Projection(p).try_into()?;
                    match &expr {
                        Expr::Alias(x, y) => selection.push(x.clone().alias(y)),
                        Expr::Wildcard => selection.push(expr),

                        // FIXME:
                        // Expr::Count => {
                        //     aggregation.push(count().alias("count"));
                        //     selection.push(col("count"));
                        // }
                        Expr::Column(_) => selection.push(expr),
                        Expr::Agg(AggExpr::Sum(sum)) => match sum.as_ref() {
                            Expr::Column(c) => {
                                let alias = format!("sum_{}", c);
                                aggregation.push(sum.clone().sum().alias(&alias));
                                selection.push(col(&alias));
                            }
                            _ => return Err(anyhow!("Unknown Column for sum, got {}", sum)),
                        },
                        Expr::Agg(AggExpr::Count(x)) => match x.as_ref() {
                            Expr::Column(c) => {
                                let alias = format!("count_{}", c);
                                aggregation.push(col(c).count().alias(&alias));
                                selection.push(col(&alias));
                            }
                            Expr::Wildcard => {
                                aggregation.push(count().alias("count"));
                                selection.push(col("count"));
                            }
                            _ => return Err(anyhow!("Unknown Column for count, got {}", x)),
                        },

                        _ => return Err(anyhow!("Unsupport projection type: {}", expr)),
                    }
                }

                let mut order_by = Vec::new();
                for expr in orders {
                    order_by.push(Order(expr).try_into()?)
                }

                let offset = offset.map(|v| Offset(v).into());
                let limit = limit.map(|v| Limit(v).into());

                Ok(Sql {
                    selection,
                    source,
                    condition,
                    group_by,
                    aggregation,
                    order_by,
                    offset,
                    limit,
                })
            }

            _ => Err(anyhow!("We only support Query at the moment")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::XQDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn parse_sql_works() {
        let url = "http://abc.xyz/abc?a=1&b=2";
        let sql = format!(
            "select a, b, c from {} where a = 1 group by c order by c desc limit 5 offset 10",
            url
        );
        let statement = &Parser::parse_sql(&XQDialect::default(), sql.as_ref()).unwrap()[0];
        let sql: Sql = statement.try_into().unwrap();

        assert_eq!(sql.source, url);
        assert_eq!(sql.limit, Some(5));
        assert_eq!(sql.offset, Some(10));
        assert_eq!(sql.order_by, vec![("c".into(), true)]);
        assert_eq!(sql.selection, vec![col("a"), col("b"), col("c")]);
        assert_eq!(sql.group_by, vec![col("c")]);
    }
}
