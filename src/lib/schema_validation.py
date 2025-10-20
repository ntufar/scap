"""
Schema validation for bronze-level data ingestion.

This module provides contract-based validation for incoming data at the bronze layer,
ensuring data conforms to expected schemas before processing.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import json
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """Represents a schema validation error."""
    field_path: str
    error_type: str
    severity: ValidationSeverity
    message: str
    expected_type: Optional[str] = None
    actual_value: Optional[Any] = None
    row_number: Optional[int] = None


@dataclass
class ValidationResult:
    """Result of schema validation."""
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]
    total_records: int
    valid_records: int
    invalid_records: int
    validation_time: datetime
    schema_name: str


class SchemaValidator:
    """Validates data against defined schemas and contracts."""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.validation_log_table = "ops.schema_validation_log"
        
        # Define schemas for different data sources
        self.schemas = self._define_default_schemas()
    
    def _define_default_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Define default schemas for common data sources."""
        return {
            "supplier_raw": {
                "source_system": "erp_system",
                "required_fields": ["supplier_id", "supplier_name", "status", "region"],
                "field_types": {
                    "supplier_id": "string",
                    "supplier_name": "string",
                    "status": "string",
                    "region": "string",
                    "contact_email": "string",
                    "phone": "string",
                    "address": "string",
                    "created_date": "timestamp",
                    "updated_date": "timestamp"
                },
                "field_constraints": {
                    "supplier_id": {"min_length": 1, "max_length": 50},
                    "supplier_name": {"min_length": 1, "max_length": 255},
                    "status": {"allowed_values": ["active", "inactive", "pending", "suspended"]},
                    "region": {"min_length": 2, "max_length": 10}
                }
            },
            "shipment_raw": {
                "source_system": "wms_system",
                "required_fields": ["shipment_id", "route_id", "carrier_id", "origin", "destination"],
                "field_types": {
                    "shipment_id": "string",
                    "route_id": "string",
                    "carrier_id": "string",
                    "origin": "string",
                    "destination": "string",
                    "planned_departure": "timestamp",
                    "actual_departure": "timestamp",
                    "planned_arrival": "timestamp",
                    "actual_arrival": "timestamp",
                    "status": "string",
                    "weight": "double",
                    "volume": "double"
                },
                "field_constraints": {
                    "shipment_id": {"min_length": 1, "max_length": 50},
                    "status": {"allowed_values": ["planned", "in_transit", "delivered", "cancelled"]},
                    "weight": {"min_value": 0.0},
                    "volume": {"min_value": 0.0}
                }
            },
            "inventory_raw": {
                "source_system": "wms_system",
                "required_fields": ["item_id", "location_id", "quantity", "snapshot_date"],
                "field_types": {
                    "item_id": "string",
                    "location_id": "string",
                    "quantity": "double",
                    "safety_stock": "double",
                    "in_transit_qty": "double",
                    "snapshot_date": "date",
                    "last_updated": "timestamp"
                },
                "field_constraints": {
                    "item_id": {"min_length": 1, "max_length": 50},
                    "location_id": {"min_length": 1, "max_length": 50},
                    "quantity": {"min_value": 0.0},
                    "safety_stock": {"min_value": 0.0},
                    "in_transit_qty": {"min_value": 0.0}
                }
            }
        }
    
    def create_validation_log_table(self) -> None:
        """Create the validation log table in Unity Catalog if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.validation_log_table} (
            validation_id STRING NOT NULL,
            schema_name STRING NOT NULL,
            source_system STRING NOT NULL,
            is_valid BOOLEAN NOT NULL,
            total_records INT NOT NULL,
            valid_records INT NOT NULL,
            invalid_records INT NOT NULL,
            error_count INT NOT NULL,
            warning_count INT NOT NULL,
            validation_time TIMESTAMP NOT NULL,
            errors_json STRING,
            created_ts TIMESTAMP NOT NULL DEFAULT current_timestamp()
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        if self.spark:
            self.spark.sql(create_table_sql)
            logger.info(f"Created validation log table: {self.validation_log_table}")
        else:
            logger.warning("No Spark session available for table creation")
    
    def validate_data(
        self, 
        data_df, 
        schema_name: str, 
        source_system: str
    ) -> ValidationResult:
        """Validate data against a defined schema."""
        start_time = datetime.utcnow()
        validation_id = f"{schema_name}_{source_system}_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        if schema_name not in self.schemas:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        schema = self.schemas[schema_name]
        errors = []
        warnings = []
        
        # Convert DataFrame to list of dictionaries for validation
        if self.spark:
            data_records = data_df.collect()
            total_records = len(data_records)
        else:
            # For non-Spark environments, assume data_df is a list of dicts
            data_records = data_df if isinstance(data_df, list) else []
            total_records = len(data_records)
        
        valid_records = 0
        invalid_records = 0
        
        for row_idx, record in enumerate(data_records):
            record_errors = self._validate_record(record, schema, row_idx)
            
            if record_errors:
                errors.extend(record_errors)
                invalid_records += 1
            else:
                valid_records += 1
        
        # Categorize errors by severity
        error_list = [e for e in errors if e.severity == ValidationSeverity.ERROR]
        warning_list = [e for e in errors if e.severity == ValidationSeverity.WARNING]
        
        is_valid = len(error_list) == 0
        
        result = ValidationResult(
            is_valid=is_valid,
            errors=error_list,
            warnings=warning_list,
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            validation_time=start_time,
            schema_name=schema_name
        )
        
        # Log validation result
        self._log_validation_result(validation_id, schema_name, source_system, result)
        
        logger.info(f"Schema validation for {schema_name}: {valid_records}/{total_records} valid records")
        return result
    
    def _validate_record(
        self, 
        record: Dict[str, Any], 
        schema: Dict[str, Any], 
        row_number: int
    ) -> List[ValidationError]:
        """Validate a single record against the schema."""
        errors = []
        
        # Check required fields
        required_fields = schema.get("required_fields", [])
        for field in required_fields:
            if field not in record or record[field] is None:
                error = ValidationError(
                    field_path=field,
                    error_type="missing_required_field",
                    severity=ValidationSeverity.ERROR,
                    message=f"Required field '{field}' is missing or null",
                    row_number=row_number
                )
                errors.append(error)
        
        # Check field types and constraints
        field_types = schema.get("field_types", {})
        field_constraints = schema.get("field_constraints", {})
        
        for field, expected_type in field_types.items():
            if field in record and record[field] is not None:
                # Type validation
                type_error = self._validate_field_type(
                    field, record[field], expected_type, row_number
                )
                if type_error:
                    errors.append(type_error)
                
                # Constraint validation
                if field in field_constraints:
                    constraint_errors = self._validate_field_constraints(
                        field, record[field], field_constraints[field], row_number
                    )
                    errors.extend(constraint_errors)
        
        return errors
    
    def _validate_field_type(
        self, 
        field: str, 
        value: Any, 
        expected_type: str, 
        row_number: int
    ) -> Optional[ValidationError]:
        """Validate that a field value matches the expected type."""
        type_mapping = {
            "string": str,
            "integer": int,
            "double": (int, float),
            "boolean": bool,
            "timestamp": (str, datetime),
            "date": (str, datetime)
        }
        
        expected_python_type = type_mapping.get(expected_type)
        if not expected_python_type:
            return None
        
        if not isinstance(value, expected_python_type):
            return ValidationError(
                field_path=field,
                error_type="type_mismatch",
                severity=ValidationSeverity.ERROR,
                message=f"Expected type '{expected_type}', got '{type(value).__name__}'",
                expected_type=expected_type,
                actual_value=str(value),
                row_number=row_number
            )
        
        return None
    
    def _validate_field_constraints(
        self, 
        field: str, 
        value: Any, 
        constraints: Dict[str, Any], 
        row_number: int
    ) -> List[ValidationError]:
        """Validate field constraints."""
        errors = []
        
        # String length constraints
        if "min_length" in constraints and isinstance(value, str):
            if len(value) < constraints["min_length"]:
                errors.append(ValidationError(
                    field_path=field,
                    error_type="min_length_violation",
                    severity=ValidationSeverity.ERROR,
                    message=f"String length {len(value)} is less than minimum {constraints['min_length']}",
                    actual_value=value,
                    row_number=row_number
                ))
        
        if "max_length" in constraints and isinstance(value, str):
            if len(value) > constraints["max_length"]:
                errors.append(ValidationError(
                    field_path=field,
                    error_type="max_length_violation",
                    severity=ValidationSeverity.ERROR,
                    message=f"String length {len(value)} exceeds maximum {constraints['max_length']}",
                    actual_value=value,
                    row_number=row_number
                ))
        
        # Numeric value constraints
        if "min_value" in constraints and isinstance(value, (int, float)):
            if value < constraints["min_value"]:
                errors.append(ValidationError(
                    field_path=field,
                    error_type="min_value_violation",
                    severity=ValidationSeverity.ERROR,
                    message=f"Value {value} is less than minimum {constraints['min_value']}",
                    actual_value=value,
                    row_number=row_number
                ))
        
        if "max_value" in constraints and isinstance(value, (int, float)):
            if value > constraints["max_value"]:
                errors.append(ValidationError(
                    field_path=field,
                    error_type="max_value_violation",
                    severity=ValidationSeverity.ERROR,
                    message=f"Value {value} exceeds maximum {constraints['max_value']}",
                    actual_value=value,
                    row_number=row_number
                ))
        
        # Allowed values constraint
        if "allowed_values" in constraints:
            if value not in constraints["allowed_values"]:
                errors.append(ValidationError(
                    field_path=field,
                    error_type="invalid_value",
                    severity=ValidationSeverity.ERROR,
                    message=f"Value '{value}' is not in allowed values {constraints['allowed_values']}",
                    actual_value=value,
                    row_number=row_number
                ))
        
        return errors
    
    def _log_validation_result(
        self, 
        validation_id: str, 
        schema_name: str, 
        source_system: str, 
        result: ValidationResult
    ) -> None:
        """Log validation result to the database."""
        if not self.spark:
            return
        
        from pyspark.sql import Row
        from pyspark.sql.functions import current_timestamp
        
        # Serialize errors for storage
        errors_json = json.dumps([
            {
                "field_path": e.field_path,
                "error_type": e.error_type,
                "severity": e.severity.value,
                "message": e.message,
                "expected_type": e.expected_type,
                "actual_value": str(e.actual_value) if e.actual_value is not None else None,
                "row_number": e.row_number
            }
            for e in result.errors + result.warnings
        ])
        
        row = Row(
            validation_id=validation_id,
            schema_name=schema_name,
            source_system=source_system,
            is_valid=result.is_valid,
            total_records=result.total_records,
            valid_records=result.valid_records,
            invalid_records=result.invalid_records,
            error_count=len(result.errors),
            warning_count=len(result.warnings),
            validation_time=result.validation_time,
            errors_json=errors_json,
            created_ts=current_timestamp()
        )
        
        df = self.spark.createDataFrame([row])
        df.write.mode("append").saveAsTable(self.validation_log_table)
    
    def add_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> None:
        """Add a new schema definition."""
        self.schemas[schema_name] = schema_definition
        logger.info(f"Added schema definition: {schema_name}")
    
    def get_validation_history(
        self, 
        schema_name: Optional[str] = None, 
        source_system: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get validation history."""
        if not self.spark:
            return []
        
        where_clause = ""
        if schema_name:
            where_clause += f" AND schema_name = '{schema_name}'"
        if source_system:
            where_clause += f" AND source_system = '{source_system}'"
        
        query = f"""
        SELECT * FROM {self.validation_log_table}
        WHERE 1=1 {where_clause}
        ORDER BY validation_time DESC
        LIMIT {limit}
        """
        
        result = self.spark.sql(query).collect()
        return [row.asDict() for row in result]
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of all defined schemas."""
        return {
            'total_schemas': len(self.schemas),
            'schemas': {
                name: {
                    'source_system': schema.get('source_system', 'unknown'),
                    'required_fields': len(schema.get('required_fields', [])),
                    'field_types': len(schema.get('field_types', {})),
                    'field_constraints': len(schema.get('field_constraints', {}))
                }
                for name, schema in self.schemas.items()
            }
        }


def create_schema_validator(spark_session) -> SchemaValidator:
    """Create a schema validator with default configuration."""
    validator = SchemaValidator(spark_session)
    validator.create_validation_log_table()
    return validator
