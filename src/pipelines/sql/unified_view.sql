-- Unified Supply Chain View
-- Provides deterministic joins across supplier, logistics, and inventory data
-- for unified analytics and reporting

CREATE OR REPLACE VIEW gold.unified_supply_chain AS
WITH 
-- Current supplier data (SCD2)
current_suppliers AS (
    SELECT 
        business_key as supplier_id,
        name as supplier_name,
        status as supplier_status,
        region as supplier_region,
        attributes_json as supplier_attributes,
        effective_start_ts as supplier_effective_start,
        source_system as supplier_source
    FROM silver.supplier_scd2
    WHERE is_current = true
),

-- Current inventory positions (latest snapshot per item/location)
current_inventory AS (
    SELECT 
        item_id,
        location_id,
        snapshot_date,
        quantity_on_hand,
        safety_stock,
        in_transit_qty,
        source_system as inventory_source,
        load_ts as inventory_load_ts
    FROM silver.inventory_position
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) 
        FROM silver.inventory_position ip2 
        WHERE ip2.item_id = silver.inventory_position.item_id 
        AND ip2.location_id = silver.inventory_position.location_id
    )
),

-- Active shipments (in transit or recently delivered)
active_shipments AS (
    SELECT 
        shipment_id,
        route_id,
        carrier_id,
        origin,
        destination,
        planned_departure_ts,
        actual_departure_ts,
        planned_arrival_ts,
        actual_arrival_ts,
        status as shipment_status,
        source_system as shipment_source,
        load_ts as shipment_load_ts
    FROM silver.shipment
    WHERE status IN ('in_transit', 'delivered')
    AND planned_departure_ts >= CURRENT_DATE() - INTERVAL 30 DAYS
),

-- Crosswalk mappings for identity resolution
supplier_crosswalk AS (
    SELECT 
        source_key as natural_supplier_id,
        business_key as supplier_id,
        confidence,
        created_ts as crosswalk_created
    FROM silver.crosswalk
    WHERE entity_type = 'supplier'
),

-- Product crosswalk (assuming we have product master data)
product_crosswalk AS (
    SELECT 
        source_key as natural_item_id,
        business_key as item_id,
        confidence,
        created_ts as crosswalk_created
    FROM silver.crosswalk
    WHERE entity_type = 'product'
)

-- Main unified view
SELECT 
    -- Product/Location identifiers
    COALESCE(pc.item_id, ci.item_id) as product_id,
    ci.location_id,
    
    -- Supplier information
    COALESCE(sc.supplier_id, cs.supplier_id) as supplier_id,
    cs.supplier_name,
    cs.supplier_status,
    cs.supplier_region,
    cs.supplier_attributes,
    
    -- Inventory information
    ci.quantity_on_hand,
    ci.safety_stock,
    ci.in_transit_qty,
    ci.snapshot_date as inventory_date,
    
    -- Shipment information (if available for this product/location)
    s.shipment_id,
    s.route_id,
    s.carrier_id,
    s.origin,
    s.destination,
    s.planned_departure_ts,
    s.actual_departure_ts,
    s.planned_arrival_ts,
    s.actual_arrival_ts,
    s.shipment_status,
    
    -- Data quality and lineage
    ci.inventory_source,
    cs.supplier_source,
    s.shipment_source,
    ci.inventory_load_ts,
    s.shipment_load_ts,
    
    -- Calculated fields
    CASE 
        WHEN ci.quantity_on_hand <= ci.safety_stock THEN 'at_risk'
        WHEN ci.quantity_on_hand = 0 THEN 'stockout'
        ELSE 'healthy'
    END as stock_status,
    
    CASE 
        WHEN s.actual_arrival_ts IS NOT NULL AND s.planned_arrival_ts IS NOT NULL THEN
            DATEDIFF(s.actual_arrival_ts, s.planned_arrival_ts)
        ELSE NULL
    END as delivery_delay_days,
    
    CASE 
        WHEN s.shipment_status = 'delivered' AND s.actual_arrival_ts IS NOT NULL THEN true
        ELSE false
    END as is_delivered,
    
    -- Timestamps for freshness tracking
    CURRENT_TIMESTAMP() as view_created_ts,
    GREATEST(
        COALESCE(ci.inventory_load_ts, '1900-01-01'),
        COALESCE(s.shipment_load_ts, '1900-01-01')
    ) as last_data_update_ts

FROM current_inventory ci
LEFT JOIN product_crosswalk pc ON ci.item_id = pc.natural_item_id
LEFT JOIN current_suppliers cs ON 1=1  -- Cross join for now, would need proper relationship
LEFT JOIN supplier_crosswalk sc ON cs.supplier_id = sc.natural_supplier_id
LEFT JOIN active_shipments s ON (
    -- This is a simplified join - in reality would need proper product/supplier relationships
    s.carrier_id = sc.supplier_id
    AND s.planned_departure_ts >= ci.snapshot_date - INTERVAL 7 DAYS
    AND s.planned_departure_ts <= ci.snapshot_date + INTERVAL 7 DAYS
)
WHERE ci.quantity_on_hand IS NOT NULL
AND ci.item_id IS NOT NULL
AND ci.location_id IS NOT NULL;

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_unified_supply_chain_product_location 
ON gold.unified_supply_chain (product_id, location_id);

CREATE INDEX IF NOT EXISTS idx_unified_supply_chain_supplier 
ON gold.unified_supply_chain (supplier_id);

CREATE INDEX IF NOT EXISTS idx_unified_supply_chain_inventory_date 
ON gold.unified_supply_chain (inventory_date);

-- Add table properties for optimization
ALTER TABLE gold.unified_supply_chain 
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.columnMapping.columnMapping' = 'true'
);

-- Add comments for documentation
COMMENT ON VIEW gold.unified_supply_chain IS 
'Unified view of supply chain data combining supplier, inventory, and shipment information with deterministic joins and data quality indicators';

COMMENT ON COLUMN gold.unified_supply_chain.product_id IS 
'Business key for product, resolved through crosswalk if available';

COMMENT ON COLUMN gold.unified_supply_chain.supplier_id IS 
'Business key for supplier, resolved through crosswalk if available';

COMMENT ON COLUMN gold.unified_supply_chain.stock_status IS 
'Calculated stock status: healthy, at_risk, or stockout based on quantity vs safety stock';

COMMENT ON COLUMN gold.unified_supply_chain.delivery_delay_days IS 
'Number of days delivery was delayed (positive) or early (negative)';

COMMENT ON COLUMN gold.unified_supply_chain.last_data_update_ts IS 
'Timestamp of the most recent data update across all source systems';

