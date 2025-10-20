from dataclasses import dataclass
from typing import List, Callable, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)


@dataclass
class FailedCheck:
    check_id: str
    severity: str  # info | warning | critical
    message: str


class DataQualityChecker:
    """Data quality checker for validating data before publishing to gold layer."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def check_table_quality(self, table_name: str) -> Dict[str, Any]:
        """Check data quality for a specific table."""
        try:
            df = self.spark.table(table_name)
            total_records = df.count()
            
            # Basic quality checks
            checks = {
                "total_records": total_records,
                "null_checks": self._check_nulls(df),
                "duplicate_checks": self._check_duplicates(df),
                "constraint_checks": self._check_constraints(df, table_name)
            }
            
            # Calculate overall quality score
            checks["quality_score"] = self._calculate_quality_score(checks)
            checks["is_valid"] = checks["quality_score"] >= 0.8
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Quality check failed for {table_name}: {str(e)}")
            return {
                "total_records": 0,
                "null_checks": {},
                "duplicate_checks": {},
                "constraint_checks": {},
                "quality_score": 0.0,
                "is_valid": False,
                "error": str(e)
            }
    
    def _check_nulls(self, df: DataFrame) -> Dict[str, int]:
        """Check for null values in required columns."""
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
        return null_counts
    
    def _check_duplicates(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate records."""
        total_count = df.count()
        distinct_count = df.distinct().count()
        duplicate_count = total_count - distinct_count
        
        return {
            "total_records": total_count,
            "distinct_records": distinct_count,
            "duplicate_count": duplicate_count,
            "duplicate_rate": duplicate_count / total_count if total_count > 0 else 0.0
        }
    
    def _check_constraints(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """Check business constraint violations."""
        constraints = {}
        
        # Check for negative values in numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if field.dataType.simpleString() in ['double', 'int', 'bigint', 'float']]
        
        for col_name in numeric_cols:
            negative_count = df.filter(df[col_name] < 0).count()
            if negative_count > 0:
                constraints[f"{col_name}_negative"] = negative_count
        
        # Table-specific constraint checks
        if "supplier" in table_name.lower():
            constraints.update(self._check_supplier_constraints(df))
        elif "inventory" in table_name.lower():
            constraints.update(self._check_inventory_constraints(df))
        elif "shipment" in table_name.lower():
            constraints.update(self._check_shipment_constraints(df))
        
        return constraints
    
    def _check_supplier_constraints(self, df: DataFrame) -> Dict[str, int]:
        """Check supplier-specific constraints."""
        constraints = {}
        
        # Check for invalid status values
        if "status" in df.columns:
            invalid_status = df.filter(~df["status"].isin(["active", "inactive", "pending", "suspended"])).count()
            if invalid_status > 0:
                constraints["invalid_status"] = invalid_status
        
        return constraints
    
    def _check_inventory_constraints(self, df: DataFrame) -> Dict[str, int]:
        """Check inventory-specific constraints."""
        constraints = {}
        
        # Check for negative quantities
        quantity_cols = ["quantity_on_hand", "safety_stock", "in_transit_qty"]
        for col in quantity_cols:
            if col in df.columns:
                negative_count = df.filter(df[col] < 0).count()
                if negative_count > 0:
                    constraints[f"{col}_negative"] = negative_count
        
        return constraints
    
    def _check_shipment_constraints(self, df: DataFrame) -> Dict[str, int]:
        """Check shipment-specific constraints."""
        constraints = {}
        
        # Check for invalid status values
        if "status" in df.columns:
            invalid_status = df.filter(~df["status"].isin(["planned", "in_transit", "delivered", "cancelled"])).count()
            if invalid_status > 0:
                constraints["invalid_status"] = invalid_status
        
        # Check for invalid timestamp relationships
        if all(col in df.columns for col in ["planned_departure_ts", "planned_arrival_ts"]):
            invalid_timestamps = df.filter(df["planned_departure_ts"] > df["planned_arrival_ts"]).count()
            if invalid_timestamps > 0:
                constraints["invalid_timestamps"] = invalid_timestamps
        
        return constraints
    
    def _calculate_quality_score(self, checks: Dict[str, Any]) -> float:
        """Calculate overall quality score (0.0 to 1.0)."""
        total_records = checks.get("total_records", 0)
        if total_records == 0:
            return 0.0
        
        score = 1.0
        
        # Deduct for null values
        null_checks = checks.get("null_checks", {})
        null_penalty = sum(null_checks.values()) / (total_records * len(null_checks)) if null_checks else 0
        score -= min(null_penalty, 0.3)  # Max 30% penalty for nulls
        
        # Deduct for duplicates
        duplicate_checks = checks.get("duplicate_checks", {})
        duplicate_rate = duplicate_checks.get("duplicate_rate", 0.0)
        score -= min(duplicate_rate, 0.2)  # Max 20% penalty for duplicates
        
        # Deduct for constraint violations
        constraint_checks = checks.get("constraint_checks", {})
        constraint_penalty = sum(constraint_checks.values()) / (total_records * len(constraint_checks)) if constraint_checks else 0
        score -= min(constraint_penalty, 0.5)  # Max 50% penalty for constraint violations
        
        return max(score, 0.0)


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


