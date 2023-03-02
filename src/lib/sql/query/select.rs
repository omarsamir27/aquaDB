
#[derive(Debug)]
pub struct SqlSelect {
    distinct: bool,
    targets: Vec<ProjectionTarget>,
    from: FromClause,
    where_clause: Option<String>,
    group_by : Option<Grouping>,
    order_by : Option<Ordering>

}

impl SqlSelect {
    pub fn new(
        distinct: bool,
        targets: Vec<ProjectionTarget>,
        from: FromClause,
        where_clause: Option<String>,
        group_by : Option<Grouping>,
        order_by : Option<Ordering>
    ) -> Self {
        Self {
            distinct,
            targets,
            from,
            where_clause,
            group_by,
            order_by
        }
    }
}

#[derive(Debug)]
pub enum ProjectionTarget {
    AllFields,
    FullyQualified(String, String),
    Shorthand(String),
}

#[derive(Debug)]
pub struct Grouping{
    criteria : Vec<ProjectionTarget>,
    having_condition : Option<String>
}

impl Grouping {
    pub fn new(criteria: Vec<ProjectionTarget>, having_condition: Option<String>) -> Self {
        Self { criteria, having_condition }
    }
}

#[derive(Debug)]
pub struct Ordering{
    criteria : Vec<ProjectionTarget>,
    descending : bool
}

impl Ordering {
    pub fn new(criteria: Vec<ProjectionTarget>, descending: bool) -> Self {
        Self { criteria, descending }
    }
}

#[derive(Debug)]
pub enum FromClause{
    Table(String),
    JoinClause(JoinClause)
}

#[derive(Debug)]
pub struct JoinClause{
    left : String,
    right : String,
    join_type : JoinType,
    join_condition : String
}

impl JoinClause {
    pub fn new(left: String, right: String, join_type:JoinType ,join_condition: String) -> Self {
        Self { left, right,join_type, join_condition }
    }
}

#[derive(Debug)]
pub enum JoinType{
    Inner,
    Left,
    Right,
    Full
}
