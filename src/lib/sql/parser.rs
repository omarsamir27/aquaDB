use crate::schema::types::CharType::VarChar;
use crate::schema::types::NumericType::{BigInt, Double, Integer, Serial, Single, SmallInt};
use crate::schema::types::Type;
use crate::sql::create_table::Constraint::{NotNull, PrimaryKey, Unique};
use crate::sql::create_table::{
    Constraint, CreateTable, CreateTableEntry, Index, IndexType, TableField,
};
use crate::sql::parser::Rule::{conditional_expression, foreign_key};
use crate::sql::query::delete::SqlDelete;
use crate::sql::query::insert::SqlInsert;
use crate::sql::query::query::{SqlQuery as QUERY, SqlValue};
use crate::sql::query::select::{FromClause, Grouping, Join, JoinClause, JoinType, Ordering, ProjectionTarget, SqlSelect};
use crate::sql::query::update::SqlUpdate;
use crate::sql::Sql;
use pest::error::ErrorVariant;
use pest::iterators::Pairs;
use pest::pratt_parser::PrattParser;
use pest_consume::{match_nodes, Error, Parser as PestParser};

type Result<T> = std::result::Result<T, Error<Rule>>;
pub type Node<'i> = pest_consume::Node<'i, Rule, ()>;

#[derive(PestParser)]
#[grammar = "../src/lib/sql/sql.pest"]
pub struct SqlParser;

//noinspection RsMethodNaming
#[pest_consume::parser]
impl SqlParser {
    fn EOI(_input: Node) -> Result<()> {
        Ok(())
    }
    fn identifier(input: Node) -> Result<String> {
        Ok(input.as_str().to_string())
    }
    // fn DISTINCT(_input: Node) -> Result<bool> {
    //     Ok(true)
    // }
    fn NULL(_input: Node) -> Result<SqlValue> {
        Ok(SqlValue::NULL)
    }
    fn table_name(input: Node) -> Result<String> {
        Ok(input.as_str().to_string())
    }
    fn projection_col(input: Node) -> Result<ProjectionTarget> {
        Ok(match_nodes!(
            input.into_children();
                        [identifier(i)] => ProjectionTarget::Shorthand(i),
            [table_name(t),identifier(i)] => ProjectionTarget::FullyQualified(t,i),
        ))
    }
    fn project_on(input: Node) -> Result<Vec<ProjectionTarget>> {
        if input.as_str() == "*" {
            Ok(vec![ProjectionTarget::AllFields])
        } else {
            Ok(match_nodes!(
                input.into_children();
                [projection_col(ids)..] => ids.collect(),
            ))
        }
    }
    fn where_clause(input: Node) -> Result<String> {
        // dbg!(&input.as_pair());
        // dbg!(&input.as_str().to_string());
        let mut txt = input.as_str().to_string();
        let txt = txt.split_once(' ').unwrap().1.to_string();
        // dbg!(txt[42..43].to_vec());
        // exit(1);
        // unreachable!();
        // txt.replace_range(,"||");

        // let mut input = input
        //     .into_children()
        //     .into_pairs()
        //     .flatten()
        //     .collect::<Vec<_>>();
        // // dbg!(&input);
        // let filtered = input
        //     .iter()
        //     .find(|n| n.as_rule() == Rule::plain_condition)
        //     .unwrap();
        // let cpy = filtered.clone().into_inner().collect::<Vec<_>>();
        // let (op, oper1, oper2) = (cpy[1].clone(), cpy[0].clone(), cpy[2].clone());
        // let x = format!("{}({},{})", op.as_str(), oper1.as_str(), oper2.as_str());
        // dbg!(x);
        // // for x in cpy{
        // //     println!("{}",x.as_str());
        // // }
        // // dbg!(cpy);
        // let where_start = input
        //     .iter()
        //     .find(|pair| pair.as_rule() == Rule::WHERE)
        //     .unwrap()
        //     .as_span()
        //     .start();
        // // dbg!(&input);
        // for n in input {
        //     let rule = n.as_rule();
        //     let str = n.as_str();
        //     if rule == Rule::OR {
        //         let span = n.as_span();
        //         let (start, end) = (span.start() - where_start, span.end() - where_start);
        //         txt.replace_range(start..end, "||")
        //     }
        // }
        // Ok(txt)
        Ok(
            match_nodes!(
                input.into_children();
                [conditional_expression(ce)] => ce
            )
        )
        // println!("{}",input.as_str());
        // let  clause = input.as_str().to_string();
        // let mut re = RegexBuilder::new("or").case_insensitive(true).build().unwrap();
        // // println!("{:?}",re);
        // let x = re.replace_all(clause.as_str(), "||").to_string();
        // dbg!(&x);
        // Ok(x)
    }
    fn conditional_expression(input: Node) -> Result<String> {
       //  let PRATT_PARSER: PrattParser<Rule> = {
       //      use pest::pratt_parser::{Assoc::*, Op};
       //      use Rule::*;
       //
       //      // Precedence is defined lowest to highest
       //      PrattParser::new()
       //          // Addition and subtract have equal precedence
       //          .op(Op::infix(OR, Left))
       //          .op(Op::infix(AND, Left))
       //  };
       // // fn parse_me(pairs:Pairs<Rule>,pratt: &PrattParser<Rule>)
        let mut txt = input.as_str().to_string().replace("AND","&&").replace("and","&&").replace("OR","||").replace("or","||");
       //  let mut input = input
       //      .into_children()
       //      .into_pairs()
       //      .flatten()
       //      .collect::<Vec<_>>();
       //  for n in input {
       //      dbg!(&n);
       //      let rule = n.as_rule();
       //      if rule == Rule::OR {
       //          let span = n.as_span();
       //          let (start, end) = (span.start(), span.end());
       //          txt.replace_range(start..end, "||")
       //      }
       //      else if rule == Rule::AND{
       //          let span = n.as_span();
       //          let (start, end) = (span.start(), span.end());
       //          dbg!(start);
       //          dbg!(end);
       //          txt.replace_range(start..end, "&&")
       //      }
       //  }

        
        Ok(txt)
    }
    fn join_type(input: Node) -> Result<JoinType> {
        if input.as_str().eq_ignore_ascii_case("left") {
            Ok(JoinType::Left)
        } else if input.as_str().eq_ignore_ascii_case("right") {
            Ok(JoinType::Right)
        } else if input.as_str().eq_ignore_ascii_case("full") {
            Ok(JoinType::Full)
        } else {
            Err(pest_consume::Error::new_from_span(
                ErrorVariant::ParsingError {
                    positives: vec![Rule::join_type],
                    negatives: vec![],
                },
                input.as_span(),
            ))
        }
    }
    fn join_clause(input: Node) -> Result<JoinClause> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t1),join_r(j)..] => JoinClause::new(t1,j.collect())
            // [table_name(t1),join_type(j),table_name(t2),conditional_expression(c)] => JoinClause::new(t1,t2,j,c),
            // [table_name(t1),table_name(t2),conditional_expression(c)] => JoinClause::new(t1,t2,JoinType::Inner,c)
        ))
    }
    fn join_r(input:Node) -> Result<Join>{
        Ok(match_nodes!(
            input.into_children();
            [join_type(jt),table_name(t2),conditional_expression(c)] => Join::new(t2,jt,Some(c)),
            [join_type(jt),table_name(t2)] => Join::new(t2,jt,None),
            [table_name(t2),conditional_expression(c)] => Join::new(t2,JoinType::Inner,Some(c)),
            [table_name(t2)] => Join::new(t2,JoinType::Inner,None)
        ))
    }

    fn table_expression(input: Node) -> Result<FromClause> {
        Ok(match_nodes!(
            input.into_children();
            [join_clause(j)] => FromClause::JoinClause(j),
            [table_name(t)] => FromClause::Table(t)
        ))
    }
    fn GROUP_BY(input: Node) -> Result<Grouping> {
        Ok(match_nodes!(
            input.into_children();
            [projection_col(p)..,HAVING(h)] => Grouping::new(p.collect(),Some(h)),
            [projection_col(p)..] => Grouping::new(p.collect(),None)
        ))
    }
    fn HAVING(input: Node) -> Result<String> {
        // stub until conditional expressions are repaired
        println!("{}", input.as_str());
        let clause = input.as_str().to_string();
        let iter = clause.split_once(' ').unwrap();
        Ok(iter.1.to_string())
    }
    fn ORDER_BY(input: Node) -> Result<Ordering> {
        let len = input.as_str().len();
        let text = input.as_str();
        let descending = text[len - 3..].eq_ignore_ascii_case("dsc");
        let cols = match_nodes!(
            input.into_children();
            [project_on(p)] => p
        );
        Ok(Ordering::new(cols, descending))
    }
    fn SqlSelect(input: Node) -> Result<SqlSelect> {
        let text = input.as_str().to_uppercase();
        let distinct = text.contains("DISTINCT");
        Ok(match_nodes!(
            input.into_children();
            [
                project_on(p),
                table_expression(t),
                where_clause(w)
            ] => SqlSelect::new(distinct,p,t,Some(w),None,None),
            [
                project_on(p),
                table_expression(t),
                where_clause(w),
                GROUP_BY(g),
                ORDER_BY(o)
            ] => SqlSelect::new(distinct,p,t,Some(w),Some(g),Some(o)),
            [
                project_on(p),
                table_expression(t),
                where_clause(w),
                GROUP_BY(g)
            ] => SqlSelect::new(distinct,p,t,Some(w),Some(g),None),
            [
                project_on(p),
                table_expression(t),
                where_clause(w),
                ORDER_BY(o)
            ] => SqlSelect::new(distinct,p,t,Some(w),None,Some(o)),
            [
                project_on(p),
                table_expression(t),
                where_clause(w)
            ] => SqlSelect::new(distinct,p,t,Some(w),None,None),
            [
                project_on(p),
                table_expression(t)
            ] => SqlSelect::new(distinct,p,t,None,None,None)
        ))
    }
    fn update_entry(input: Node) -> Result<(String, SqlValue)> {
        Ok(match_nodes!(
            input.into_children();
            [identifier(i),sql_value(v)] => (i,v)
        ))
    }
    fn SqlUpdate(input: Node) -> Result<SqlUpdate> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t),update_entry(u)..,where_clause(w)] => SqlUpdate::new(t,u.collect(),Some(w)),
            [table_name(t),update_entry(u)..] => SqlUpdate::new(t,u.collect(),None)
        ))
    }
    fn insert_cols(input: Node) -> Result<Vec<String>> {
        Ok(match_nodes!(
            input.into_children();
            [identifier(i)..] => i.collect()
        ))
    }
    fn sql_value(input: Node) -> Result<SqlValue> {
        Ok(match_nodes!(
            input.into_children();
            [NULL(_)] => SqlValue::NULL,
            [constant(c)] => c

        ))
    }
    fn constant(input: Node) -> Result<SqlValue> {
        Ok(match_nodes!(
            input.into_children();
            [numeric_constant(n)] => n,
            [string_literal(s)] => s,
            [TRUE(_)] => SqlValue::Bool(true),
            [FALSE(_)] => SqlValue::Bool(false)
        ))
    }
    fn TRUE(_input: Node) -> Result<SqlValue> {
        Ok(SqlValue::Bool(true))
    }
    fn FALSE(_input: Node) -> Result<SqlValue> {
        Ok(SqlValue::Bool(false))
    }
    fn numeric_constant(input: Node) -> Result<SqlValue> {
        Ok(SqlValue::Numeric(input.as_str().to_string()))
    }
    fn string_literal(input: Node) -> Result<SqlValue> {
        let input = input.as_str().trim_start_matches('\"').trim_end_matches('\"');
        Ok(SqlValue::Text(input.to_string()))
    }
    fn insert_vals(input: Node) -> Result<Vec<SqlValue>> {
        Ok(match_nodes!(
            input.into_children();
            [sql_value(sv)..] => sv.collect()
        ))
    }
    fn SqlInsert(input: Node) -> Result<SqlInsert> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t),insert_cols(c),insert_vals(v)] => SqlInsert::new(t,c,v)
        ))
    }
    fn SqlDelete(input: Node) -> Result<SqlDelete> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t),where_clause(w)] => SqlDelete::new(t,Some(w)),
            [table_name(t)] => SqlDelete::new(t,None)
        ))
    }
    fn SqlQuery(input: Node) -> Result<QUERY> {
        Ok(match_nodes!(
            input.into_children();
            [SqlSelect(s)] => QUERY::SELECT(s),
            [SqlUpdate(u)] => QUERY::UPDATE(u),
            [SqlDelete(d)] => QUERY::DELETE(d),
            [SqlInsert(i)] => QUERY::INSERT(i),
        ))
    }
    pub fn Sql(input: Node) -> Result<Sql> {
        Ok(match_nodes!(
            input.into_children();
            [SqlQuery(q),EOI(_)] => Sql::new_query(q),
            [SqlCreateTable(ct),EOI(_)] => Sql::new_table(ct)
        ))
    }

    fn datatype(input: Node) -> Result<Type> {
        Ok(match input.into_children().single().unwrap().as_rule() {
            Rule::SMALLINT => Type::Numeric(SmallInt),
            Rule::INTEGER => Type::Numeric(Integer),
            Rule::BIGINT => Type::Numeric(BigInt),
            Rule::SINGLE => Type::Numeric(Single),
            Rule::DOUBLE => Type::Numeric(Double),
            Rule::SERIAL => Type::Numeric(Serial),
            Rule::VARCHAR => Type::Character(VarChar),
            _ => unreachable!(),
        })
    }
    fn foreign_key(input: Node) -> Result<Constraint> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t),identifier(i)] => Constraint::References(t,i)
        ))
    }
    fn constraint(input: Node) -> Result<Constraint> {
        Ok(
            match input.clone().into_children().single().unwrap().as_rule() {
                Rule::primary_key => PrimaryKey,
                Rule::not_null => NotNull,
                Rule::unique => Unique,
                Rule::foreign_key => {
                    SqlParser::foreign_key(input.into_children().single().unwrap())?
                }
                _ => unreachable!(),
            },
        )
    }

    fn table_col(input: Node) -> Result<TableField> {
        Ok(match_nodes!(
            input.into_children();
            [identifier(i),datatype(d),constraint(c)..] => TableField::new(i,d,c.collect())
        ))
    }

    fn index_method(input: Node) -> Result<IndexType> {
        Ok(match input.as_str().to_ascii_lowercase().as_str() {
            "hash" => IndexType::Hash,
            "btree" => IndexType::Btree,
            _ => unreachable!(),
        })
        // IndexType::from_str(input.as_str())
    }

    fn index(input: Node) -> Result<Index> {
        Ok(match_nodes!(
            input.into_children();
            [index_method(im),identifier(idx_name),identifier(idx_fields)..] => Index::new(idx_name,idx_fields.collect(),im)
        ))
    }
    fn createTableEntry(input: Node) -> Result<CreateTableEntry> {
        Ok(match_nodes!(
            input.into_children();
            [table_col(tc)] => CreateTableEntry::TableField(tc),
            [index(i)] => CreateTableEntry::Index(i)
        ))
    }
    fn SqlCreateTable(input: Node) -> Result<CreateTable> {
        Ok(match_nodes!(
            input.into_children();
            [table_name(t),createTableEntry(cte)..] => CreateTable::new(t,cte.collect()),
            // [table_name(t),table_col(tc)..,index(i)..] => CreateTable::new(t,tc.collect(),i.c),
        ))
    }
}

pub fn parse_query(query: &str) -> Result<Sql> {
    let select = <SqlParser as pest_consume::Parser>::parse(Rule::Sql, query)?;
    // dbg!(&select);
    let x = select.single()?;
    SqlParser::Sql(x)
}
