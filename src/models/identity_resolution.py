"""
Identity resolution functions for the Navigator Supply Chain Lakehouse.

This module provides deterministic identity resolution across different source systems
using crosswalks and survivorship rules based on trusted sources and effective dates.
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum

from .crosswalk import CrosswalkManager, CrosswalkRecord

logger = logging.getLogger(__name__)


class TrustLevel(Enum):
    """Trust levels for source systems in identity resolution."""
    HIGH = 1
    MEDIUM = 2
    LOW = 3


@dataclass
class IdentityResolutionRule:
    """Defines rules for identity resolution."""
    trusted_sources: List[str]  # Ordered by trust level (highest first)
    survivorship_field: str  # Field to use for tie-breaking
    survivorship_order: str  # 'asc' or 'desc' for ordering


class IdentityResolver:
    """Handles identity resolution using crosswalks and survivorship rules."""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.crosswalk_manager = CrosswalkManager(spark_session)
        
        # Default trust levels for common source systems
        self.trust_levels = {
            'erp_system': TrustLevel.HIGH,
            'wms_system': TrustLevel.MEDIUM,
            'external_api': TrustLevel.LOW,
            'manual_entry': TrustLevel.HIGH,
            'legacy_system': TrustLevel.LOW
        }
        
        # Default resolution rules by entity type
        self.resolution_rules = {
            'supplier': IdentityResolutionRule(
                trusted_sources=['erp_system', 'manual_entry', 'wms_system'],
                survivorship_field='effective_start_ts',
                survivorship_order='desc'
            ),
            'product': IdentityResolutionRule(
                trusted_sources=['erp_system', 'manual_entry', 'wms_system'],
                survivorship_field='created_ts',
                survivorship_order='desc'
            ),
            'location': IdentityResolutionRule(
                trusted_sources=['wms_system', 'erp_system', 'manual_entry'],
                survivorship_field='created_ts',
                survivorship_order='desc'
            ),
            'shipment': IdentityResolutionRule(
                trusted_sources=['wms_system', 'erp_system'],
                survivorship_field='load_ts',
                survivorship_order='desc'
            )
        }
    
    def resolve_entity_identity(
        self, 
        entity_type: str, 
        source_records: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve entity identity from multiple source records using survivorship rules.
        
        Args:
            entity_type: Type of entity being resolved ('supplier', 'product', etc.)
            source_records: List of records from different source systems
            
        Returns:
            Resolved entity record with canonical business key and best attributes
        """
        if not source_records:
            logger.warning(f"No source records provided for {entity_type} resolution")
            return None
        
        # Get resolution rules for this entity type
        rules = self.resolution_rules.get(entity_type)
        if not rules:
            logger.warning(f"No resolution rules defined for entity type: {entity_type}")
            return None
        
        # Group records by business key (if already resolved)
        business_key_groups = {}
        unresolved_records = []
        
        for record in source_records:
            source_system = record.get('source_system', 'unknown')
            source_key = record.get('source_key', '')
            
            # Try to resolve business key
            business_key = self.crosswalk_manager.resolve_business_key(
                source_system, source_key, entity_type
            )
            
            if business_key:
                if business_key not in business_key_groups:
                    business_key_groups[business_key] = []
                business_key_groups[business_key].append(record)
            else:
                unresolved_records.append(record)
        
        # If we have resolved groups, pick the best one
        if business_key_groups:
            best_business_key = self._select_best_business_key(
                business_key_groups, rules
            )
            best_record = self._apply_survivorship_rules(
                business_key_groups[best_business_key], rules
            )
            return best_record
        
        # If no resolved groups, try to create new business key from unresolved records
        if unresolved_records:
            return self._create_new_identity(entity_type, unresolved_records, rules)
        
        logger.warning(f"No valid records found for {entity_type} resolution")
        return None
    
    def _select_best_business_key(
        self, 
        business_key_groups: Dict[str, List[Dict]], 
        rules: IdentityResolutionRule
    ) -> str:
        """Select the best business key from multiple groups based on trust levels."""
        best_key = None
        best_trust_score = float('inf')
        
        for business_key, records in business_key_groups.items():
            # Calculate trust score for this business key group
            trust_score = self._calculate_trust_score(records, rules.trusted_sources)
            
            if trust_score < best_trust_score:
                best_trust_score = trust_score
                best_key = business_key
        
        return best_key
    
    def _calculate_trust_score(
        self, 
        records: List[Dict], 
        trusted_sources: List[str]
    ) -> float:
        """Calculate trust score for a group of records."""
        if not records:
            return float('inf')
        
        # Find the highest trust level source in this group
        best_source_rank = float('inf')
        
        for record in records:
            source_system = record.get('source_system', 'unknown')
            if source_system in trusted_sources:
                rank = trusted_sources.index(source_system)
                best_source_rank = min(best_source_rank, rank)
        
        return best_source_rank if best_source_rank != float('inf') else len(trusted_sources)
    
    def _apply_survivorship_rules(
        self, 
        records: List[Dict], 
        rules: IdentityResolutionRule
    ) -> Dict[str, Any]:
        """Apply survivorship rules to select the best record from a group."""
        if len(records) == 1:
            return records[0]
        
        # Sort by survivorship field
        survivorship_field = rules.survivorship_field
        reverse = rules.survivorship_order == 'desc'
        
        # Filter records that have the survivorship field
        valid_records = [
            r for r in records 
            if survivorship_field in r and r[survivorship_field] is not None
        ]
        
        if not valid_records:
            # Fallback to first record if no survivorship field
            logger.warning(f"No records have survivorship field '{survivorship_field}', using first record")
            return records[0]
        
        # Sort by survivorship field
        sorted_records = sorted(
            valid_records,
            key=lambda x: x[survivorship_field],
            reverse=reverse
        )
        
        best_record = sorted_records[0]
        logger.debug(f"Selected record with {survivorship_field}={best_record[survivorship_field]}")
        
        return best_record
    
    def _create_new_identity(
        self, 
        entity_type: str, 
        records: List[Dict], 
        rules: IdentityResolutionRule
    ) -> Optional[Dict[str, Any]]:
        """Create a new business identity from unresolved records."""
        if not records:
            return None
        
        # Apply survivorship rules to get the best record
        best_record = self._apply_survivorship_rules(records, rules)
        
        # Generate a new business key
        source_system = best_record.get('source_system', 'unknown')
        source_key = best_record.get('source_key', '')
        new_business_key = f"{entity_type.upper()}_{source_system}_{source_key}"
        
        # Add crosswalk mapping
        self.crosswalk_manager.add_crosswalk_mapping(
            source_system, source_key, entity_type, new_business_key
        )
        
        # Update the record with the new business key
        best_record['business_key'] = new_business_key
        best_record['resolved_ts'] = datetime.utcnow()
        
        logger.info(f"Created new business key {new_business_key} for {entity_type}")
        
        return best_record
    
    def batch_resolve_identities(
        self, 
        entity_type: str, 
        source_records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Resolve identities for a batch of records."""
        resolved_records = []
        
        # Group records by potential business key (using source_key as proxy)
        key_groups = {}
        for record in source_records:
            source_key = record.get('source_key', '')
            if source_key not in key_groups:
                key_groups[source_key] = []
            key_groups[source_key].append(record)
        
        # Resolve each group
        for source_key, group_records in key_groups.items():
            resolved = self.resolve_entity_identity(entity_type, group_records)
            if resolved:
                resolved_records.append(resolved)
        
        logger.info(f"Resolved {len(resolved_records)} out of {len(source_records)} {entity_type} records")
        return resolved_records
    
    def get_identity_resolution_stats(self) -> Dict[str, Any]:
        """Get statistics about identity resolution performance."""
        if not self.spark:
            return {}
        
        # Get crosswalk statistics
        crosswalk_stats = self.crosswalk_manager.validate_crosswalk_integrity()
        
        # Get resolution rules summary
        rules_summary = {}
        for entity_type, rule in self.resolution_rules.items():
            rules_summary[entity_type] = {
                'trusted_sources': rule.trusted_sources,
                'survivorship_field': rule.survivorship_field,
                'survivorship_order': rule.survivorship_order
            }
        
        return {
            'crosswalk_stats': crosswalk_stats,
            'resolution_rules': rules_summary,
            'trust_levels': {k: v.value for k, v in self.trust_levels.items()}
        }


def create_identity_resolver(spark_session, entity_types: List[str] = None) -> IdentityResolver:
    """Create an identity resolver with default configuration."""
    resolver = IdentityResolver(spark_session)
    
    if entity_types:
        # Filter resolution rules to only include specified entity types
        resolver.resolution_rules = {
            k: v for k, v in resolver.resolution_rules.items() 
            if k in entity_types
        }
    
    return resolver
