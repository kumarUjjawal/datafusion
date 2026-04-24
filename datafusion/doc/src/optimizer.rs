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

use std::fmt::Write;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerRuleDoc {
    pub name: String,
    pub summary: String,
    pub applies_when: String,
    pub phase: Option<String>,
    pub notes: Vec<String>,
}

impl OptimizerRuleDoc {
    pub fn new(
        name: impl Into<String>,
        summary: impl Into<String>,
        applies_when: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            summary: summary.into(),
            applies_when: applies_when.into(),
            phase: None,
            notes: vec![],
        }
    }

    pub fn with_phase(mut self, phase: impl Into<String>) -> Self {
        self.phase = Some(phase.into());
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }
}

pub fn render_optimizer_rule_docs(
    analyzer_rules: &[OptimizerRuleDoc],
    logical_rules: &[OptimizerRuleDoc],
    physical_rules: &[OptimizerRuleDoc],
) -> String {
    let mut docs = String::new();

    let _ = writeln!(docs, "# Optimizer Rules Reference");
    let _ = writeln!(docs);
    let _ = writeln!(
        docs,
        "This page lists the built-in rules in DataFusion's default analyzer, logical optimizer, and physical optimizer pipelines."
    );
    let _ = writeln!(docs);
    let _ = writeln!(
        docs,
        "The rule names shown here match the names shown by `EXPLAIN VERBOSE`."
    );
    let _ = writeln!(docs);
    let _ = writeln!(
        docs,
        "Rule order matters. The default pipeline may change between releases, so this page is a release-specific reference."
    );
    let _ = writeln!(docs);

    render_section(
        &mut docs,
        "Analyzer Rules",
        "These rules run before logical optimization. They prepare the logical plan for later planning and optimization stages.",
        "analyzer",
        analyzer_rules,
    );
    render_section(
        &mut docs,
        "Logical Optimizer Rules",
        "These rules rewrite `LogicalPlan` nodes while preserving query results.",
        "logical",
        logical_rules,
    );
    render_section(
        &mut docs,
        "Physical Optimizer Rules",
        "These rules rewrite `ExecutionPlan` nodes after physical planning, usually to satisfy requirements or reduce runtime cost.",
        "physical",
        physical_rules,
    );

    docs
}

fn render_section(
    docs: &mut String,
    title: &str,
    description: &str,
    stage: &str,
    rules: &[OptimizerRuleDoc],
) {
    let _ = writeln!(docs, "## {title}");
    let _ = writeln!(docs);
    let _ = writeln!(docs, "{description}");
    let _ = writeln!(docs);
    let _ = writeln!(docs, "| order | rule name | phase | summary |");
    let _ = writeln!(docs, "| --- | --- | --- | --- |");

    for (idx, rule) in rules.iter().enumerate() {
        let anchor = rule_anchor(stage, idx);
        let name = format!("[`{}`](#{anchor})", rule.name);
        let phase = rule.phase.as_deref().unwrap_or("-");
        let _ = writeln!(
            docs,
            "| {} | {} | {} | {} |",
            idx + 1,
            name,
            escape_table_cell(phase),
            escape_table_cell(&rule.summary)
        );
    }

    let _ = writeln!(docs);

    for (idx, rule) in rules.iter().enumerate() {
        let anchor = rule_anchor(stage, idx);
        let _ = writeln!(docs, "<a id=\"{anchor}\"></a>");
        let _ = writeln!(docs);
        match &rule.phase {
            Some(phase) => {
                let _ = writeln!(docs, "### `{}` ({phase})", rule.name);
                let _ = writeln!(docs);
                let _ = writeln!(docs, "_Seen in `EXPLAIN VERBOSE` as `{}`._", rule.name);
            }
            None => {
                let _ = writeln!(docs, "### `{}`", rule.name);
            }
        }
        let _ = writeln!(docs);
        let _ = writeln!(docs, "{}", rule.summary);
        let _ = writeln!(docs);
        let _ = writeln!(docs, "#### Applies When");
        let _ = writeln!(docs);
        let _ = writeln!(docs, "{}", rule.applies_when);
        let _ = writeln!(docs);

        if !rule.notes.is_empty() {
            let _ = writeln!(docs, "#### Notes");
            let _ = writeln!(docs);
            for note in &rule.notes {
                let _ = writeln!(docs, "- {note}");
            }
            let _ = writeln!(docs);
        }
    }
}

fn rule_anchor(stage: &str, idx: usize) -> String {
    format!("{stage}-rule-{}", idx + 1)
}

fn escape_table_cell(value: &str) -> String {
    value.replace('|', r"\|")
}

#[cfg(test)]
mod tests {
    use super::{OptimizerRuleDoc, render_optimizer_rule_docs};

    #[test]
    fn render_optimizer_rule_docs_includes_sections_and_phases() {
        let analyzer_rules = vec![OptimizerRuleDoc::new(
            "type_coercion",
            "Adds needed casts.",
            "Types need coercion.",
        )];
        let logical_rules = vec![OptimizerRuleDoc::new(
            "push_down_filter",
            "Moves filters earlier.",
            "A filter can move lower.",
        )];
        let physical_rules = vec![
            OptimizerRuleDoc::new(
                "ProjectionPushdown",
                "Runs early.",
                "Columns can be pruned.",
            )
            .with_phase("early pass")
            .with_note("This pass runs before later TopK and limit rewrites."),
            OptimizerRuleDoc::new(
                "ProjectionPushdown",
                "Runs late.",
                "Late rewrites exposed more pruning.",
            )
            .with_phase("late pass"),
        ];

        let docs =
            render_optimizer_rule_docs(&analyzer_rules, &logical_rules, &physical_rules);

        assert!(docs.contains("# Optimizer Rules Reference"));
        assert!(docs.contains("## Analyzer Rules"));
        assert!(docs.contains("## Logical Optimizer Rules"));
        assert!(docs.contains("## Physical Optimizer Rules"));
        assert!(docs.contains("### `ProjectionPushdown` (early pass)"));
        assert!(docs.contains("### `ProjectionPushdown` (late pass)"));
        assert!(docs.contains("[`ProjectionPushdown`](#physical-rule-1)"));
        assert!(docs.contains("[`ProjectionPushdown`](#physical-rule-2)"));
        assert!(docs.contains("#### Notes"));
    }
}
