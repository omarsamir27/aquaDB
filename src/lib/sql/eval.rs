// use exmex::prelude::*;
// use exmex::{BinOp, Operator, ops_factory, MakeOperators, Val};
//
// // fn LIKE(text:&str,pattern:&str) -> bool{
// //     text.is
// // }
//
//  ops_factory!(
//     StringOpsFactory,
//     Val,
//     Operator::make_bin(
//         "LIKE",
//         BinOp{
//             apply : |a,b| a == b,
//             prio : 2,
//             is_commutative : false
//         }
//     )
// );
