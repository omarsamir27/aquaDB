use crate::schema::types::{NumericType, Type};
use crate::FieldId;
use evalexpr::Value::Boolean;
use evalexpr::{FloatType, Node as ExprTree, Node, Operator, Value};
use std::collections::HashMap;

#[inline(always)]
pub fn extract_used_variables(tree: &ExprTree) -> Vec<String> {
    tree.iter_variable_identifiers()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
}

#[inline(always)]
pub fn var_only_equality(tree: &ExprTree, var: &str) -> bool {
    tree.iter()
        .filter(|n| is_binary_expr(n))
        .any(|n| node_has_var_with_other_op(n, var, Operator::Eq))
}

#[inline(always)]
fn node_has_var_with_other_op(node: &ExprTree, var: &str, op: Operator) -> bool {
    node.iter_variable_identifiers().any(|v| v == var) && *node.operator() != op
}

#[inline(always)]
fn is_binary_expr(node: &ExprTree) -> bool {
    let children = node.children();
    children.len() == 2 && children[0].children().is_empty() && children[1].children().is_empty()
}

#[inline(always)]
fn binary_expr_has_var(node: &ExprTree, var: &str) -> bool {
    node.iter_variable_identifiers().any(|v| v == var)
}

#[inline(always)]
pub fn replace_vars_map(root: &mut ExprTree, map: &HashMap<String, FieldId>) {
    let mut iter = root.children_mut();
    iter.iter_mut().for_each(|n| replace_vars_map(n, map));
    if let Operator::VariableIdentifierRead { identifier } = root.operator_mut() {
        let v = map.get(identifier.as_str());
        if v.is_some() {
            let _ = std::mem::replace(
                root.operator_mut(),
                Operator::VariableIdentifierRead {
                    identifier: v.unwrap().to_string(),
                },
            );
        }
    }
}

pub fn get_all_binary_clauses(root: &ExprTree) -> Vec<Node> {
    fn add_to_vec(root: &ExprTree, data: &mut Vec<ExprTree>) {
        if is_binary_expr(root) {
            data.push(root.clone());
            return;
        }
        let mut iter = root.children();
        for n in iter {
            add_to_vec(n, data);
        }
    }
    let mut iter = root.children();
    let mut bin_clauses = vec![];
    for n in iter {
        add_to_vec(n, &mut bin_clauses);
    }
    bin_clauses
}

pub fn get_single_binary_clause(root: &ExprTree) -> (Operator, Value) {
    let op = root.operator().clone();
    let val = root
        .children()
        .iter()
        .filter(|n| matches!(n.operator(), Operator::Const { .. }))
        .map(|n| {
            if let Operator::Const { value } = n.operator() {
                value.clone()
            } else {
                unreachable!()
            }
        })
        .next()
        .unwrap();
    (op, val)
}

pub fn set_node_true(root: &mut ExprTree, node: &ExprTree) {
    let mut iter = root.children_mut();
    iter.iter_mut().for_each(|n| set_node_true(n, node));
    if *root == *node {
        set_true(root);
    }
}
fn set_node_const(node: &mut ExprTree, val: evalexpr::Operator) {
    std::mem::replace(node.operator_mut(), val);
    std::mem::take(node.children_mut());
}

fn set_true(node: &mut ExprTree) {
    set_node_const(
        node,
        Operator::Const {
            value: Boolean(true),
        },
    )
}

fn set_false(node: &mut ExprTree) {
    set_node_const(
        node,
        Operator::Const {
            value: Boolean(false),
        },
    )
}

pub fn simplify(root: &mut ExprTree) {
    let mut iter = root.children_mut();
    for n in iter {
        simplify(n);
        // if !is_binary_expr(n){
        //     simplify(n);
        // }
    }
    let op = root.operator().clone();
    iter = root.children_mut();
    if op == Operator::And {
        if iter.iter().any(is_false) {
            set_false(root);
            return;
        }
        iter.retain(|child| !is_true(child));
        if iter.is_empty() {
            set_true(root);
        }
    } else if op == Operator::Or {
        if iter.iter().any(is_true) {
            set_true(root);
            return;
        }
        iter.retain(|child| !is_false(child));
        if iter.is_empty() {
            set_false(root);
        }
    }
}

#[inline(always)]
fn is_const_val(node: &ExprTree, val: evalexpr::Operator) -> bool {
    *node.operator() == val && node.children().is_empty()
}

fn is_true(node: &ExprTree) -> bool {
    is_const_val(
        node,
        Operator::Const {
            value: Boolean(true),
        },
    )
}

fn is_false(node: &ExprTree) -> bool {
    is_const_val(
        node,
        Operator::Const {
            value: Boolean(false),
        },
    )
}

pub fn value_as_bytes(val: &evalexpr::Value, key_type: Type) -> Vec<u8> {
    match val {
        Value::String(a) => a.as_bytes().to_vec(),
        Value::Float(f) => match key_type {
            Type::Numeric(n) => match n {
                NumericType::Single => (*f as f32).to_ne_bytes().to_vec(),
                NumericType::Double => ((*f).to_ne_bytes()).to_vec(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        Value::Int(i) => match key_type {
            Type::Numeric(n) => match n {
                NumericType::SmallInt => (*i as i16).to_ne_bytes().to_vec(),
                NumericType::Integer => (*i as i32).to_ne_bytes().to_vec(),
                NumericType::BigInt => (*i as i64).to_ne_bytes().to_vec(),
                NumericType::Serial => (*i as i32).to_ne_bytes().to_vec(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}
