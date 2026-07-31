"""Deterministic execution of the foundation quality-rule set."""

from __future__ import annotations

from collections.abc import Callable

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

    def evaluate(
        self,
        inputs: CanonicalPartitions,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> tuple[QualityIssue, ...]:
        issues: list[QualityIssue] = []
        for rule in (
            primary_key_issues,
            required_value_issues,
            daily_bar_value_issues,
            coverage_issues,
            financial_availability_issues,
            cross_partition_schema_issues,
        ):
            heartbeat()
            issues.extend(rule(inputs))
        heartbeat()
        return tuple(
            sorted(issues, key=lambda issue: (issue.dataset.value, issue.rule_id))
        )
