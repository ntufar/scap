# Data Model: Navigator Supply Chain Lakehouse

## Entities

### Supplier (SCD2)
- Keys: natural_key (source key), business_key (canonical)
- Attributes: name, status, region, attributes_json
- Audit: effective_start_ts, effective_end_ts, is_current, source_system, load_ts
- Rules: identity resolution via crosswalk; survivorship by trusted source then latest effective date

### Shipment
- Keys: shipment_id (business), route_id, carrier_id
- Attributes: origin, destination, planned_departure_ts, actual_departure_ts, planned_arrival_ts, actual_arrival_ts, status
- Relationships: references Supplier (carrier_id or supplier as vendor), references Product via item codes in line items
- Audit: load_ts, source_system

### InventoryPosition
- Keys: item_id (business), location_id, snapshot_date
- Attributes: quantity_on_hand, safety_stock, in_transit_qty
- Relationships: references Product, references Location
- Audit: load_ts, source_system

### CostReference
- Keys: item_id, effective_start_ts
- Attributes: cost_currency, unit_cost, effective_end_ts, is_current
- Relationships: references Product
- Audit: load_ts, source_system

### Crosswalk (Identity Resolution)
- Keys: source_system, source_key, entity_type, business_key
- Attributes: confidence (default 1.0 for deterministic), created_ts

## Relationships
- Supplier 1..* Shipment (via carrier/vendor)
- Product 1..* InventoryPosition
- Product 1..* CostReference (SCD2)
- Location 1..* InventoryPosition

## Validation Rules
- Supplier natural_key must map to exactly one business_key (deterministic)
- Shipment timestamps: planned <= actual where populated; no negative durations
- InventoryPosition quantities must be non-negative; safety_stock >= 0
- Crosswalk must be unique on (source_system, source_key, entity_type)

## State Transitions
- Supplier: SCD2 changes open a new current row and close prior row
- Shipment: statuses flow planned → in_transit → delivered | cancelled
- InventoryPosition: daily snapshots; latest by snapshot_date per item/location


