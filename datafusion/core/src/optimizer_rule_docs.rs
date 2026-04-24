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

use datafusion_doc::{OptimizerRuleDoc, render_optimizer_rule_docs};

pub(crate) fn render_default_optimizer_rule_docs() -> String {
    render_optimizer_rule_docs(
        &analyzer_rule_docs(),
        &logical_rule_docs(),
        &physical_rule_docs(),
    )
}

fn analyzer_rule_docs() -> Vec<OptimizerRuleDoc> {
    vec![
        OptimizerRuleDoc::new(
            "resolve_grouping_function",
            "Rewrites `GROUPING(...)` calls into expressions over DataFusion's internal grouping id.",
            "The plan contains `GROUPING` expressions produced by `GROUPING SETS`, `ROLLUP`, or `CUBE`.",
        ),
        OptimizerRuleDoc::new(
            "type_coercion",
            "Adds implicit casts so operators and functions receive valid input types.",
            "Expressions mix types that must be coerced to a common or expected type before planning can continue.",
        ),
    ]
}

fn logical_rule_docs() -> Vec<OptimizerRuleDoc> {
    vec![
        OptimizerRuleDoc::new(
            "rewrite_set_comparison",
            "Rewrites `ANY` and `ALL` set-comparison subqueries into `EXISTS`-based boolean expressions with correct SQL NULL semantics.",
            "A filter or expression contains a set comparison such as `= ANY`, `< ALL`, or similar constructs.",
        ),
        OptimizerRuleDoc::new(
            "optimize_unions",
            "Flattens nested unions and removes unions with a single input.",
            "The logical plan contains `UNION` nodes, including nested unions or distinct-over-union shapes.",
        ),
        OptimizerRuleDoc::new(
            "simplify_expressions",
            "Constant-folds and simplifies expressions while preserving output names.",
            "Expressions contain literals, redundant boolean logic, removable casts, or other simplifiable patterns.",
        ),
        OptimizerRuleDoc::new(
            "replace_distinct_aggregate",
            "Rewrites `DISTINCT` and `DISTINCT ON` operators into aggregate-based plans that later rules can optimize further.",
            "The plan contains logical `Distinct` operators.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_join",
            "Replaces keyless inner joins with a literal `false` filter by an empty relation.",
            "An inner join has no join keys and its join filter is the literal boolean `false`.",
        ),
        OptimizerRuleDoc::new(
            "decorrelate_predicate_subquery",
            "Converts eligible `IN` and `EXISTS` predicate subqueries into semi or anti joins.",
            "A filter contains predicate subqueries that can be decorrelated into joins.",
        ),
        OptimizerRuleDoc::new(
            "scalar_subquery_to_join",
            "Rewrites eligible scalar subqueries into joins and adds schema-preserving projections.",
            "A filter or projection contains scalar subqueries that can be decorrelated into joins.",
        ),
        OptimizerRuleDoc::new(
            "decorrelate_lateral_join",
            "Rewrites eligible lateral joins into regular joins.",
            "The plan contains lateral joins with correlated input that can be pulled up safely.",
        ),
        OptimizerRuleDoc::new(
            "extract_equijoin_predicate",
            "Splits join filters into equijoin keys and residual predicates.",
            "A join filter contains equality predicates that can become explicit join keys.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_duplicated_expr",
            "Removes duplicate expressions from projections, aggregates, and similar operators.",
            "The same logical expression appears more than once in a plan node input list.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_filter",
            "Drops always-true filters and replaces always-false or NULL filters with empty relations.",
            "A filter predicate simplifies to a constant boolean value or NULL.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_cross_join",
            "Uses filter predicates to replace cross joins with inner joins when join keys can be found.",
            "A filter above a cross join exposes equijoin conditions between the inputs.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_limit",
            "Removes no-op limits and simplifies trivial limit shapes.",
            "The plan contains limits that can be proven redundant or can collapse to simpler forms.",
        ),
        OptimizerRuleDoc::new(
            "propagate_empty_relation",
            "Pushes empty-relation knowledge upward so operators fed by no rows collapse early.",
            "Any subtree is already known to return no rows.",
        ),
        OptimizerRuleDoc::new(
            "filter_null_join_keys",
            "Adds `IS NOT NULL` filters to nullable equijoin keys that can never match.",
            "A join uses equijoin keys with `NULL != NULL` semantics and a nullable side can be filtered early.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_outer_join",
            "Rewrites outer joins to inner joins when later filters reject the NULL-extended rows.",
            "A filter above a left, right, or full outer join makes unmatched rows impossible.",
        ),
        OptimizerRuleDoc::new(
            "push_down_limit",
            "Moves literal limits closer to scans and unions and merges adjacent limits.",
            "The plan contains literal `LIMIT` or `OFFSET` values that can be applied earlier.",
        ),
        OptimizerRuleDoc::new(
            "push_down_filter",
            "Moves filters as early as possible through filter-commutative operators.",
            "A filter can be pushed below projections, joins, aggregates, scans, or other supported operators.",
        ),
        OptimizerRuleDoc::new(
            "single_distinct_aggregation_to_group_by",
            "Rewrites single-column `DISTINCT` aggregations into two-stage `GROUP BY` plans.",
            "All distinct aggregates operate on the same input expression and the remaining aggregates fit the rule's supported patterns.",
        ),
        OptimizerRuleDoc::new(
            "eliminate_group_by_constant",
            "Removes constant or functionally redundant expressions from `GROUP BY`.",
            "Grouping expressions contain constants or values already determined by other grouping keys.",
        ),
        OptimizerRuleDoc::new(
            "common_sub_expression_eliminate",
            "Computes repeated subexpressions once and reuses the result.",
            "The same expression is repeated within a projection, aggregate, sort, or window operator.",
        ),
        OptimizerRuleDoc::new(
            "extract_leaf_expressions",
            "Pulls cheap leaf expressions closer to data sources so later pruning and filter rules can act earlier.",
            "Expressions such as field access or other leaf extraction can be materialized lower in the plan.",
        ),
        OptimizerRuleDoc::new(
            "push_down_leaf_projections",
            "Pushes the helper projections created by leaf extraction toward leaf inputs.",
            "The previous leaf-extraction pass introduced helper projections that can move lower in the plan.",
        ),
        OptimizerRuleDoc::new(
            "optimize_projections",
            "Prunes unused columns and removes unnecessary logical projections.",
            "Downstream operators do not need all columns produced by a subtree.",
        ),
    ]
}

fn physical_rule_docs() -> Vec<OptimizerRuleDoc> {
    vec![
        OptimizerRuleDoc::new(
            "OutputRequirements",
            "Adds helper nodes so output requirements survive later physical rewrites.",
            "Physical optimization starts and the plan's top-level output requirements need to stay attached to the tree.",
        )
        .with_phase("add phase")
        .with_note(
            "This pass inserts temporary `OutputRequirementExec` nodes that a later remove phase strips away.",
        ),
        OptimizerRuleDoc::new(
            "aggregate_statistics",
            "Uses exact source statistics to answer some aggregates without scanning data.",
            "An aggregate result can be derived directly from exact source statistics.",
        ),
        OptimizerRuleDoc::new(
            "join_selection",
            "Chooses join implementation, build side, and partition mode from statistics and stream properties.",
            "The plan contains joins whose implementation or side ordering can be improved.",
        ),
        OptimizerRuleDoc::new(
            "LimitedDistinctAggregation",
            "Pushes limit hints into grouped distinct-style aggregations when only a small result is needed.",
            "A grouped aggregation has no aggregate expressions or ordering requirements and a downstream limit means fewer groups are enough.",
        ),
        OptimizerRuleDoc::new(
            "FilterPushdown",
            "Pushes supported physical filters down toward data sources before distribution and sorting are enforced.",
            "The plan contains `FilterExec` nodes or parent operators that can pass filters to their children.",
        )
        .with_phase("pre-optimization phase"),
        OptimizerRuleDoc::new(
            "EnforceDistribution",
            "Adds repartitioning only where needed to satisfy physical distribution requirements.",
            "Operators require a specific input partitioning or can benefit from added parallelism.",
        ),
        OptimizerRuleDoc::new(
            "CombinePartialFinalAggregate",
            "Collapses adjacent partial and final aggregates when the distributed shape makes them redundant.",
            "A partial aggregate feeds a matching final aggregate that can be combined safely.",
        ),
        OptimizerRuleDoc::new(
            "EnforceSorting",
            "Adds or removes local sorts to satisfy required input orderings.",
            "Operators depend on a particular local ordering.",
        ),
        OptimizerRuleDoc::new(
            "OptimizeAggregateOrder",
            "Updates aggregate expressions to use the best ordering once sort requirements are known.",
            "Aggregate expressions can take advantage of input ordering chosen by earlier physical passes.",
        ),
        OptimizerRuleDoc::new(
            "WindowTopN",
            "Replaces eligible row-number window and filter patterns with per-partition TopK execution.",
            "A window plan computes `ROW_NUMBER` and a following filter keeps only the first `K` rows per partition.",
        ),
        OptimizerRuleDoc::new(
            "ProjectionPushdown",
            "Pushes projections toward inputs before later physical rewrites add more limit and TopK structure.",
            "The plan carries columns that downstream operators do not need.",
        )
        .with_phase("early pass"),
        OptimizerRuleDoc::new(
            "OutputRequirements",
            "Removes the temporary output-requirement helper nodes after requirement-sensitive planning is done.",
            "The early distribution, sorting, and projection passes have finished using the helper nodes.",
        )
        .with_phase("remove phase"),
        OptimizerRuleDoc::new(
            "LimitAggregation",
            "Passes a limit hint into eligible aggregations so they can keep fewer accumulator buckets.",
            "A fetched sort orders by a grouped key or MIN/MAX aggregate output and only top rows are needed.",
        ),
        OptimizerRuleDoc::new(
            "LimitPushPastWindows",
            "Pushes fetch limits through bounded window operators when doing so keeps the result correct.",
            "A downstream limit is blocked by a bounded window operator but can still move lower safely.",
        ),
        OptimizerRuleDoc::new(
            "HashJoinBuffering",
            "Adds buffering on the probe side of hash joins so probing can start before build completion.",
            "The plan contains hash joins and buffering can improve runtime overlap.",
        ),
        OptimizerRuleDoc::new(
            "LimitPushdown",
            "Moves physical limits into child operators or fetch-enabled variants to cut data early.",
            "A physical plan contains limit nodes or operators that can absorb a fetch.",
        ),
        OptimizerRuleDoc::new(
            "TopKRepartition",
            "Pushes TopK below hash repartition when the partition key is a prefix of the sort key.",
            "A fetched sort sits above hash repartition and the key relationship makes pre-shuffle TopK valid.",
        ),
        OptimizerRuleDoc::new(
            "ProjectionPushdown",
            "Runs projection pushdown again after limit and TopK rewrites expose new pruning opportunities.",
            "Later physical passes created new removable columns or projection merge opportunities.",
        )
        .with_phase("late pass"),
        OptimizerRuleDoc::new(
            "PushdownSort",
            "Pushes sort requirements into data sources that can already return sorted output.",
            "A required ordering can be satisfied directly by a scan or other source operator.",
        ),
        OptimizerRuleDoc::new(
            "EnsureCooperative",
            "Wraps non-cooperative plan parts so long-running tasks yield fairly.",
            "Some execution operators would otherwise not yield cooperatively.",
        ),
        OptimizerRuleDoc::new(
            "FilterPushdown(Post)",
            "Pushes dynamic filters at the end of optimization, after plan references stop moving.",
            "The plan contains dynamic filters or late filter opportunities that depend on final plan references.",
        )
        .with_phase("post-optimization phase")
        .with_note(
            "This late pass protects dynamic filter references that earlier rewrites could invalidate.",
        ),
        OptimizerRuleDoc::new(
            "SanityCheckPlan",
            "Validates that the final physical plan meets ordering, distribution, and infinite-input safety requirements.",
            "Physical optimization is finishing and the final plan must be checked before execution.",
        )
        .with_note("This is a validation pass and does not rewrite the plan."),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        analyzer_rule_docs, logical_rule_docs, physical_rule_docs,
        render_default_optimizer_rule_docs,
    };
    use datafusion_optimizer::analyzer::Analyzer;
    use datafusion_optimizer::optimizer::Optimizer;
    use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;

    #[test]
    fn analyzer_rule_docs_match_default_rule_order() {
        let rules: Vec<_> = Analyzer::new()
            .rules
            .iter()
            .map(|rule| rule.name().to_string())
            .collect();
        let docs: Vec<_> = analyzer_rule_docs()
            .into_iter()
            .map(|doc| doc.name)
            .collect();

        assert_eq!(docs, rules);
    }

    #[test]
    fn logical_rule_docs_match_default_rule_order() {
        let rules: Vec<_> = Optimizer::new()
            .rules
            .iter()
            .map(|rule| rule.name().to_string())
            .collect();
        let docs: Vec<_> = logical_rule_docs()
            .into_iter()
            .map(|doc| doc.name)
            .collect();

        assert_eq!(docs, rules);
    }

    #[test]
    fn physical_rule_docs_match_default_rule_order() {
        let rules: Vec<_> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|rule| rule.name().to_string())
            .collect();
        let docs: Vec<_> = physical_rule_docs()
            .into_iter()
            .map(|doc| doc.name)
            .collect();

        assert_eq!(docs, rules);
    }

    #[test]
    fn render_optimizer_rule_docs_smoke_test() {
        let docs = render_default_optimizer_rule_docs();

        assert!(docs.contains("# Optimizer Rules Reference"));
        assert!(docs.contains("### `eliminate_join`"));
        assert!(docs.contains(
            "Replaces keyless inner joins with a literal `false` filter by an empty relation."
        ));
    }
}
