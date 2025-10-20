"""
Freshness status management for the Navigator Supply Chain Lakehouse.

This module tracks data freshness across different domains (supplier, logistics, inventory)
and provides SLA monitoring and alerting capabilities.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FreshnessStatus(Enum):
    """Freshness status levels."""
    OK = "ok"
    WARNING = "warning"
    BREACH = "breach"


@dataclass
class DomainSLA:
    """SLA configuration for a data domain."""
    domain: str
    target_sla_minutes: int
    warning_threshold_minutes: int
    breach_threshold_minutes: int
    description: str = ""


@dataclass
class FreshnessRecord:
    """Represents a freshness status record for a domain."""
    domain: str
    last_updated: datetime
    target_sla_minutes: int
    warning_threshold_minutes: int
    breach_threshold_minutes: int
    status: FreshnessStatus
    age_minutes: int
    created_ts: datetime = None
    updated_ts: datetime = None
    
    def __post_init__(self):
        if self.created_ts is None:
            self.created_ts = datetime.utcnow()
        if self.updated_ts is None:
            self.updated_ts = datetime.utcnow()


class FreshnessManager:
    """Manages data freshness tracking and SLA monitoring."""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.freshness_table = "ops.freshness_status"
        
        # Default SLA configurations for different domains
        self.domain_slas = {
            'supplier': DomainSLA(
                domain='supplier',
                target_sla_minutes=24 * 60,  # 24 hours
                warning_threshold_minutes=20 * 60,  # 20 hours
                breach_threshold_minutes=25 * 60,  # 25 hours
                description="Supplier data should be updated daily"
            ),
            'logistics': DomainSLA(
                domain='logistics',
                target_sla_minutes=60,  # 1 hour
                warning_threshold_minutes=45,  # 45 minutes
                breach_threshold_minutes=75,  # 75 minutes
                description="Logistics data should be updated hourly"
            ),
            'inventory': DomainSLA(
                domain='inventory',
                target_sla_minutes=5,  # 5 minutes
                warning_threshold_minutes=3,  # 3 minutes
                breach_threshold_minutes=8,  # 8 minutes
                description="Inventory data should be updated near real-time"
            )
        }
    
    def create_freshness_table(self) -> None:
        """Create the freshness status table in Unity Catalog if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.freshness_table} (
            domain STRING NOT NULL,
            last_updated TIMESTAMP NOT NULL,
            target_sla_minutes INT NOT NULL,
            warning_threshold_minutes INT NOT NULL,
            breach_threshold_minutes INT NOT NULL,
            status STRING NOT NULL,
            age_minutes INT NOT NULL,
            created_ts TIMESTAMP NOT NULL,
            updated_ts TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        if self.spark:
            self.spark.sql(create_table_sql)
            logger.info(f"Created freshness status table: {self.freshness_table}")
        else:
            logger.warning("No Spark session available for table creation")
    
    def update_freshness_status(
        self, 
        domain: str, 
        last_updated: datetime,
        custom_sla: Optional[DomainSLA] = None
    ) -> FreshnessRecord:
        """Update freshness status for a specific domain."""
        # Get SLA configuration
        sla = custom_sla or self.domain_slas.get(domain)
        if not sla:
            raise ValueError(f"No SLA configuration found for domain: {domain}")
        
        # Calculate age in minutes
        now = datetime.utcnow()
        age_minutes = int((now - last_updated).total_seconds() / 60)
        
        # Determine status based on age
        if age_minutes <= sla.warning_threshold_minutes:
            status = FreshnessStatus.OK
        elif age_minutes <= sla.breach_threshold_minutes:
            status = FreshnessStatus.WARNING
        else:
            status = FreshnessStatus.BREACH
        
        # Create freshness record
        record = FreshnessRecord(
            domain=domain,
            last_updated=last_updated,
            target_sla_minutes=sla.target_sla_minutes,
            warning_threshold_minutes=sla.warning_threshold_minutes,
            breach_threshold_minutes=sla.breach_threshold_minutes,
            status=status,
            age_minutes=age_minutes
        )
        
        # Store in database
        if self.spark:
            self._store_freshness_record(record)
        
        logger.info(f"Updated freshness status for {domain}: {status.value} (age: {age_minutes} minutes)")
        return record
    
    def _store_freshness_record(self, record: FreshnessRecord) -> None:
        """Store a freshness record in the database."""
        from pyspark.sql import Row
        from pyspark.sql.functions import current_timestamp
        
        row = Row(
            domain=record.domain,
            last_updated=record.last_updated,
            target_sla_minutes=record.target_sla_minutes,
            warning_threshold_minutes=record.warning_threshold_minutes,
            breach_threshold_minutes=record.breach_threshold_minutes,
            status=record.status.value,
            age_minutes=record.age_minutes,
            created_ts=record.created_ts,
            updated_ts=current_timestamp()
        )
        
        df = self.spark.createDataFrame([row])
        df.write.mode("overwrite").saveAsTable(self.freshness_table)
    
    def get_freshness_status(self, domain: Optional[str] = None) -> List[FreshnessRecord]:
        """Get freshness status for one or all domains."""
        if not self.spark:
            logger.warning("No Spark session available for freshness status retrieval")
            return []
        
        if domain:
            query = f"""
            SELECT * FROM {self.freshness_table}
            WHERE domain = '{domain}'
            ORDER BY updated_ts DESC
            LIMIT 1
            """
        else:
            query = f"""
            SELECT * FROM {self.freshness_table}
            ORDER BY domain, updated_ts DESC
            """
        
        result = self.spark.sql(query).collect()
        
        records = []
        for row in result:
            record = FreshnessRecord(
                domain=row['domain'],
                last_updated=row['last_updated'],
                target_sla_minutes=row['target_sla_minutes'],
                warning_threshold_minutes=row['warning_threshold_minutes'],
                breach_threshold_minutes=row['breach_threshold_minutes'],
                status=FreshnessStatus(row['status']),
                age_minutes=row['age_minutes'],
                created_ts=row['created_ts'],
                updated_ts=row['updated_ts']
            )
            records.append(record)
        
        return records
    
    def get_breach_alerts(self) -> List[FreshnessRecord]:
        """Get all domains that are currently in breach status."""
        all_status = self.get_freshness_status()
        return [record for record in all_status if record.status == FreshnessStatus.BREACH]
    
    def get_warning_alerts(self) -> List[FreshnessRecord]:
        """Get all domains that are currently in warning status."""
        all_status = self.get_freshness_status()
        return [record for record in all_status if record.status == FreshnessStatus.WARNING]
    
    def check_domain_freshness(self, domain: str) -> Optional[FreshnessRecord]:
        """Check freshness for a specific domain by looking up the latest data timestamp."""
        if not self.spark:
            logger.warning("No Spark session available for domain freshness check")
            return None
        
        # Get the latest timestamp for this domain from the appropriate table
        latest_timestamp = self._get_latest_timestamp_for_domain(domain)
        if not latest_timestamp:
            logger.warning(f"No data found for domain: {domain}")
            return None
        
        # Update freshness status
        return self.update_freshness_status(domain, latest_timestamp)
    
    def _get_latest_timestamp_for_domain(self, domain: str) -> Optional[datetime]:
        """Get the latest timestamp for a domain from the appropriate table."""
        # Map domains to their corresponding tables
        domain_tables = {
            'supplier': 'silver.supplier',
            'logistics': 'silver.shipment',
            'inventory': 'silver.inventory_position'
        }
        
        table_name = domain_tables.get(domain)
        if not table_name:
            logger.warning(f"No table mapping found for domain: {domain}")
            return None
        
        # Query for the latest timestamp
        query = f"""
        SELECT MAX(load_ts) as latest_ts
        FROM {table_name}
        """
        
        try:
            result = self.spark.sql(query).collect()
            if result and result[0]['latest_ts']:
                return result[0]['latest_ts']
        except Exception as e:
            logger.error(f"Error querying latest timestamp for {domain}: {e}")
        
        return None
    
    def refresh_all_domains(self) -> Dict[str, FreshnessRecord]:
        """Refresh freshness status for all configured domains."""
        results = {}
        
        for domain in self.domain_slas.keys():
            try:
                record = self.check_domain_freshness(domain)
                if record:
                    results[domain] = record
            except Exception as e:
                logger.error(f"Error refreshing freshness for domain {domain}: {e}")
        
        return results
    
    def get_freshness_summary(self) -> Dict[str, any]:
        """Get a summary of freshness status across all domains."""
        all_status = self.get_freshness_status()
        
        summary = {
            'total_domains': len(all_status),
            'ok_domains': len([r for r in all_status if r.status == FreshnessStatus.OK]),
            'warning_domains': len([r for r in all_status if r.status == FreshnessStatus.WARNING]),
            'breach_domains': len([r for r in all_status if r.status == FreshnessStatus.BREACH]),
            'domains': {}
        }
        
        for record in all_status:
            summary['domains'][record.domain] = {
                'status': record.status.value,
                'age_minutes': record.age_minutes,
                'target_sla_minutes': record.target_sla_minutes,
                'last_updated': record.last_updated.isoformat()
            }
        
        return summary
    
    def add_domain_sla(self, sla: DomainSLA) -> None:
        """Add or update SLA configuration for a domain."""
        self.domain_slas[sla.domain] = sla
        logger.info(f"Added SLA configuration for domain: {sla.domain}")
    
    def get_sla_configuration(self, domain: str) -> Optional[DomainSLA]:
        """Get SLA configuration for a specific domain."""
        return self.domain_slas.get(domain)


def create_freshness_manager(spark_session, domains: List[str] = None) -> FreshnessManager:
    """Create a freshness manager with default configuration."""
    manager = FreshnessManager(spark_session)
    
    if domains:
        # Filter SLA configurations to only include specified domains
        manager.domain_slas = {
            k: v for k, v in manager.domain_slas.items() 
            if k in domains
        }
    
    return manager
