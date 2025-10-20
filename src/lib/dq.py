from dataclasses import dataclass
from typing import List, Callable


@dataclass
class FailedCheck:
    check_id: str
    severity: str  # info | warning | critical
    message: str


def assert_non_negative(value: float, check_id: str) -> List[FailedCheck]:
    if value < 0:
        return [FailedCheck(check_id=check_id, severity="critical", message="Value must be non-negative")]
    return []


def publish_gate(failed_checks: List[FailedCheck]) -> bool:
    return not any(fc.severity == "critical" for fc in failed_checks)


def run_checks(rows: List[dict], rules: List[Callable[[dict], List[FailedCheck]]]) -> List[FailedCheck]:
    failures: List[FailedCheck] = []
    for row in rows:
        for rule in rules:
            failures.extend(rule(row))
    return failures


