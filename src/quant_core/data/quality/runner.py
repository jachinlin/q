"""Deterministic execution of the foundation quality-rule set."""

from __future__ import annotations

from quant_core.data.quality.models import QualityIssue
from quant_core.data.quality.rules import (
    CanonicalPartitions,
    coverage_issues,
    cross_partition_schema_issues,
    daily_bar_value_issues,
    financial_availability_issues,
    primary_key_issues,
    required_value_issues,
)


class QualityRunner:
    """Evaluate all foundation checks and return stable issue ordering."""

    def evaluate(self, inputs: CanonicalPartitions) -> tuple[QualityIssue, ...]:
        issues = [
            *primary_key_issues(inputs),
            *required_value_issues(inputs),
            *daily_bar_value_issues(inputs),
            *coverage_issues(inputs),
            *financial_availability_issues(inputs),
            *cross_partition_schema_issues(inputs),
        ]
        return tuple(
            sorted(issues, key=lambda issue: (issue.dataset.value, issue.rule_id))
        )
