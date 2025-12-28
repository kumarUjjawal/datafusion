// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{
    array::{ArrayRef, Int64Array},
    error::ArrowError,
};
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;

use crate::utils::make_scalar_function;
use datafusion_common::{Result, arrow_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

/// Pre-computed factorial values for integers 0-20.
/// factorial(n) for n > 20 overflows i64.
/// This table is shared with the Spark factorial implementation.
pub const FACTORIALS: [i64; 21] = [
    1,                    // 0!
    1,                    // 1!
    2,                    // 2!
    6,                    // 3!
    24,                   // 4!
    120,                  // 5!
    720,                  // 6!
    5040,                 // 7!
    40320,                // 8!
    362880,               // 9!
    3628800,              // 10!
    39916800,             // 11!
    479001600,            // 12!
    6227020800,           // 13!
    87178291200,          // 14!
    1307674368000,        // 15!
    20922789888000,       // 16!
    355687428096000,      // 17!
    6402373705728000,     // 18!
    121645100408832000,   // 19!
    2432902008176640000,  // 20!
];

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Factorial. Returns 1 if value is less than 2.",
    syntax_example = "factorial(numeric_expression)",
    sql_example = r#"```sql
> SELECT factorial(5);
+---------------+
| factorial(5)  |
+---------------+
| 120           |
+---------------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FactorialFunc {
    signature: Signature,
}

impl Default for FactorialFunc {
    fn default() -> Self {
        FactorialFunc::new()
    }
}

impl FactorialFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FactorialFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "factorial"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(factorial, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Factorial SQL function
/// 
/// Uses the pre-computed `FACTORIALS` lookup table for O(1) performance.
/// Behavior:
/// - Values less than 2: returns 1
/// - Values 2-20: returns factorial from lookup table
/// - Values > 20: returns overflow error
pub fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Int64 => {
            let arg = downcast_named_arg!((&args[0]), "value", Int64Array);
            Ok(arg
                .iter()
                .map(|a| match a {
                    Some(a) if a < 2 => Ok(Some(1i64)),
                    Some(a) if a <= 20 => Ok(Some(FACTORIALS[a as usize])),
                    Some(a) => Err(arrow_datafusion_err!(ArrowError::ComputeError(
                        format!("Overflow happened on FACTORIAL({a})")
                    ))),
                    None => Ok(None),
                })
                .collect::<Result<Int64Array>>()
                .map(Arc::new)? as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function factorial."),
    }
}

#[cfg(test)]
mod test {

    use datafusion_common::cast::as_int64_array;

    use super::*;

    #[test]
    fn test_factorial_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 4])), // input
        ];

        let result = factorial(&args).expect("failed to initialize function factorial");
        let ints =
            as_int64_array(&result).expect("failed to initialize function factorial");

        let expected = Int64Array::from(vec![1, 1, 2, 24]);

        assert_eq!(ints, &expected);
    }
}
