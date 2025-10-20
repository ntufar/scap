# Architecture: Navigator Supply Chain Lakehouse

## Overview

The Navigator Supply Chain Lakehouse is a Databricks-based data platform that unifies supplier, logistics, and inventory data into a conformed analytics layer with deterministic identity resolution and strong governance/observability. The architecture follows the medallion pattern (bronze → silver → gold) with comprehensive data quality gates and freshness monitoring.

## High-Level Architecture

### Medallion Architecture Overview
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Data Sources                                          │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   ERP System    │   WMS System    │  External APIs  │    Manual Entry         │
│  (Suppliers)    │  (Logistics)    │   (Inventory)   │   (Reference Data)      │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Bronze Layer (Raw Data)                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│  • Schema Validation (Contracts)                                               │
│  • Data Ingestion (PySpark Jobs)                                               │
│  • Raw Data Storage (Delta Lake)                                               │
│  • Source System Tracking                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Silver Layer (Conformed Data)                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│  • Identity Resolution (Crosswalks)                                            │
│  • Data Conformance (SCD2, Keys)                                               │
│  • Late-Arriving Updates Handling                                              │
│  • Data Quality Checks                                                          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Gold Layer (Curated Data)                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│  • KPI Aggregations                                                            │
│  • Business Views                                                              │
│  • Performance Optimization (OPTIMIZE/ZORDER)                                 │
│  • Access Controls (Unity Catalog)                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        API Layer (Contracts)                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  • Unified Records Endpoint                                                    │
│  • Freshness Status Endpoint                                                   │
│  • Data Quality Status Endpoint                                                │
│  • SQL Functions & Notebooks                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Component Interaction Diagram
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           External Systems                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │    ERP      │  │     WMS     │  │   External  │  │   Manual    │          │
│  │   System    │  │   System    │  │     APIs    │  │   Entry     │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Layer                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Schema Validator                                     │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │  Supplier   │  │  Shipment   │  │  Inventory  │  │  Reference  │    │    │
│  │  │  Contract   │  │  Contract   │  │  Contract   │  │  Contract   │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Bronze Storage (Delta Lake)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  supplier   │  │  shipment   │  │ inventory   │  │  reference  │          │
│  │    raw      │  │    raw      │  │    raw      │  │    raw      │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Data Processing Layer                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Identity Resolver                                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │ Crosswalk   │  │ Survivorship│  │ Trust Level │  │ Resolution  │    │    │
│  │  │  Manager    │  │   Rules     │  │ Management  │  │   Engine    │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Silver Storage (Delta Lake)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  supplier   │  │  shipment   │  │ inventory   │  │  reference  │          │
│  │ (SCD2)      │  │ (conformed) │  │ (conformed) │  │ (conformed) │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Quality Gate Layer                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Publish Gate                                        │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │Completeness │  │ Uniqueness  │  │Referential  │  │   Range     │    │    │
│  │  │   Checks    │  │   Checks    │  │ Integrity   │  │   Checks    │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Gold Storage (Delta Lake)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  unified    │  │    kpi      │  │  supplier   │  │  logistics  │          │
│  │   view      │  │  metrics    │  │   summary   │  │   summary   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        API & Access Layer                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Unified    │  │ Freshness   │  │     DQ      │  │    SQL      │          │
│  │  Records    │  │   Status    │  │   Status    │  │ Functions   │          │
│  │  Endpoint   │  │  Endpoint   │  │  Endpoint   │  │ & Notebooks │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Sequence Diagram
```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Source    │  │   Schema    │  │   Bronze    │  │   Silver    │  │    Gold     │
│  System     │  │ Validator   │  │   Layer     │  │   Layer     │  │   Layer     │
└─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
       │                │                │                │                │
       │ 1. Send Data   │                │                │                │
       ├───────────────►│                │                │                │
       │                │                │                │                │
       │                │ 2. Validate    │                │                │
       │                │    Schema      │                │                │
       │                ├───────────────►│                │                │
       │                │                │                │                │
       │                │ 3. Store Raw   │                │                │
       │                │    Data        │                │                │
       │                ├───────────────►│                │                │
       │                │                │                │                │
       │                │                │ 4. Identity   │                │
       │                │                │    Resolution │                │
       │                │                ├───────────────►│                │
       │                │                │                │                │
       │                │                │ 5. Conform    │                │
       │                │                │    Data       │                │
       │                │                ├───────────────►│                │
       │                │                │                │                │
       │                │                │                │ 6. Quality    │
       │                │                │                │    Gate       │
       │                │                │                ├───────────────►│
       │                │                │                │                │
       │                │                │                │ 7. Publish    │
       │                │                │                │    Curated    │
       │                │                │                │    Data       │
       │                │                │                ├───────────────►│
```

### Freshness Monitoring Architecture
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Freshness Monitoring System                             │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Supplier   │  │ Logistics   │  │  Inventory  │  │  Reference  │          │
│  │   Domain    │  │   Domain    │  │   Domain    │  │   Domain    │          │
│  │ (24h SLA)   │  │ (1h SLA)    │  │ (5min SLA)  │  │ (24h SLA)   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Freshness Manager                                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │   Status    │  │    SLA      │  │   Breach    │  │   Alert     │    │    │
│  │  │  Tracking   │  │ Monitoring  │  │ Detection   │  │ Generation  │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Status Storage (Delta Lake)                         │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │    OK       │  │  WARNING    │  │   BREACH    │  │  HISTORICAL │    │    │
│  │  │  Status     │  │  Status     │  │  Status     │  │   Trends    │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Alerting & Dashboard Layer                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Databricks │  │   Email     │  │   Slack     │  │  Custom     │          │
│  │   Alerts    │  │ Notifications│  │ Notifications│  │ Dashboard   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Identity Resolution Process Flow
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Identity Resolution Process                              │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Source    │  │   Source    │  │   Source    │  │   Source    │          │
│  │  Record 1   │  │  Record 2   │  │  Record 3   │  │  Record N   │          │
│  │ (ERP System)│  │ (WMS System)│  │ (External)  │  │ (Manual)    │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Crosswalk Lookup                                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │  Source     │  │  Source     │  │  Source     │  │  Source     │    │    │
│  │  │  Key 1      │  │  Key 2      │  │  Key 3      │  │  Key N      │    │    │
│  │  │     ↓       │  │     ↓       │  │     ↓       │  │     ↓       │    │    │
│  │  │ Business    │  │ Business    │  │ Business    │  │ Business    │    │    │
│  │  │  Key 1      │  │  Key 2      │  │  Key 3      │  │  Key N      │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Group by Business Key                               │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │  Group 1    │  │  Group 2    │  │  Group 3    │  │  Group N    │    │    │
│  │  │ (Key: BK1)  │  │ (Key: BK2)  │  │ (Key: BK3)  │  │ (Key: BKN)  │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Survivorship Rules                                  │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │  Trust      │  │  Effective  │  │  Confidence │  │  Best       │    │    │
│  │  │  Level      │  │    Date     │  │   Score     │  │  Record     │    │    │
│  │  │  Ranking    │  │  Sorting    │  │  Selection  │  │ Selection   │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ Resolved    │  │ Resolved    │  │ Resolved    │  │ Resolved    │          │
│  │  Record 1   │  │  Record 2   │  │  Record 3   │  │  Record N   │          │
│  │ (Canonical) │  │ (Canonical) │  │ (Canonical) │  │ (Canonical) │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Quality Gate Process
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Data Quality Gate Process                               │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Silver    │  │   Silver    │  │   Silver    │  │   Silver    │          │
│  │  Dataset 1  │  │  Dataset 2  │  │  Dataset 3  │  │  Dataset N  │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Quality Check Engine                                │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │Completeness │  │ Uniqueness  │  │Referential  │  │   Range     │    │    │
│  │  │   Checks    │  │   Checks    │  │ Integrity   │  │   Checks    │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Severity Classification                             │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │  CRITICAL   │  │  WARNING    │  │     INFO    │  │    PASS     │    │    │
│  │  │  (Block)    │  │  (Allow)    │  │  (Allow)    │  │  (Allow)    │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Gate Decision                                       │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │   BLOCK     │  │    OPEN     │  │    OPEN     │  │    OPEN     │    │    │
│  │  │ Publication │  │ Publication │  │ Publication │  │ Publication │    │    │
│  │  │   + Alert   │  │  + Warning  │  │  + Info     │  │   + Log     │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Unity Catalog Security Model
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Unity Catalog Security Model                            │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Analyst   │  │  Engineer   │  │   Auditor   │  │  External   │          │
│  │    Role     │  │    Role     │  │    Role     │  │   Partner   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Permission Matrix                                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │    Gold     │  │   Silver    │  │   Bronze    │  │     Ops     │    │    │
│  │  │   Layer     │  │   Layer     │  │   Layer     │  │   Layer     │    │    │
│  │  │             │  │             │  │             │  │             │    │    │
│  │  │ Analyst: R  │  │ Analyst: -  │  │ Analyst: -  │  │ Analyst: -  │    │    │
│  │  │Engineer: RW │  │Engineer: RW │  │Engineer: R  │  │Engineer: R  │    │    │
│  │  │ Auditor: R  │  │ Auditor: R  │  │ Auditor: R  │  │ Auditor: R  │    │    │
│  │  │External: R  │  │External: -  │  │External: -  │  │External: -  │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Data Masking & Row-Level Security                   │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │ Sensitive   │  │  Regional   │  │  Temporal   │  │  Custom     │    │    │
│  │  │   Data      │  │  Filtering  │  │  Filtering  │  │  Policies   │    │    │
│  │  │  Masking    │  │             │  │             │  │             │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Performance Optimization Strategy
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Performance Optimization Strategy                       │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Partitioning│  │  Clustering │  │  Indexing   │  │  Caching    │          │
│  │             │  │             │  │             │  │             │          │
│  │ • By Date   │  │ • ZORDER    │  │ • Primary   │  │ • Delta     │          │
│  │ • By Key    │  │ • HASH      │  │   Keys      │  │   Cache     │          │
│  │ • By Region │  │ • RANGE     │  │ • Foreign   │  │ • Query     │          │
│  │             │  │             │  │   Keys      │  │   Cache     │          │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘          │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Query Optimization                                 │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │ Predicate   │  │   Join      │  │  Aggregation│  │   Vector    │    │    │
│  │  │ Pushdown    │  │  Ordering   │  │  Pushdown   │  │ ization     │    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│         │                 │                 │                 │              │
│         ▼                 ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    Resource Management                                 │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │    │
│  │  │   Cluster   │  │   Memory    │  │   CPU       │  │   Storage   │    │    │
│  │  │ Auto-scaling│  │  Tuning     │  │  Allocation │  │  Optimization│    │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Data Models (`src/models/`)

#### Crosswalk Model (`crosswalk.py`)
- **Purpose**: Deterministic identity resolution across source systems
- **Key Features**:
  - Maps source keys to canonical business keys
  - Confidence scoring for resolution quality
  - Support for multiple entity types
  - Integrity validation and duplicate detection

```python
# Example usage
crosswalk_manager = CrosswalkManager(spark)
crosswalk_manager.add_crosswalk_mapping(
    source_system="erp_system",
    source_key="SUP001", 
    entity_type="supplier",
    business_key="SUP_ACME_CORP"
)
```

#### Identity Resolution (`identity_resolution.py`)
- **Purpose**: Resolves entity identities using survivorship rules
- **Key Features**:
  - Trust level management for source systems
  - Survivorship by trusted source + effective date
  - Batch processing capabilities
  - Custom resolution rules per entity type

```python
# Example usage
resolver = IdentityResolver(spark)
resolved_record = resolver.resolve_entity_identity(
    entity_type="supplier",
    source_records=[record1, record2, record3]
)
```

#### Freshness Management (`freshness.py`)
- **Purpose**: Tracks data freshness and SLA compliance
- **Key Features**:
  - Domain-specific SLA configurations
  - Automatic freshness status updates
  - Breach detection and alerting
  - Historical tracking

```python
# Example usage
freshness_manager = FreshnessManager(spark)
status = freshness_manager.update_freshness_status(
    domain="inventory",
    last_updated=datetime.utcnow()
)
```

### 2. Data Quality & Governance (`src/lib/`)

#### Publish Gate (`publish_gate.py`)
- **Purpose**: Controls data publication based on quality checks
- **Key Features**:
  - Severity-based blocking (CRITICAL blocks, WARNING allows)
  - Comprehensive quality checks (completeness, uniqueness, referential integrity)
  - Audit logging and history
  - Custom check definitions

```python
# Example usage
gate = PublishGate(spark)
result = gate.evaluate_gate(dataset="silver", table="supplier")
if not result.can_publish:
    print(f"Publication blocked: {result.message}")
```

#### Schema Validation (`schema_validation.py`)
- **Purpose**: Validates incoming data against contracts
- **Key Features**:
  - Contract-based validation at ingestion
  - Field type and constraint validation
  - Multi-source schema support
  - Detailed error reporting

```python
# Example usage
validator = SchemaValidator(spark)
result = validator.validate_data(
    data_df=incoming_data,
    schema_name="supplier_raw",
    source_system="erp_system"
)
```

### 3. Data Pipelines (`src/pipelines/`)

#### Bronze Jobs (`bronze_jobs.py`)
- **Purpose**: Raw data ingestion and initial validation
- **Process**:
  1. Ingest data from source systems
  2. Apply schema validation
  3. Store in bronze layer with metadata
  4. Track ingestion timestamps

#### Silver Jobs (`silver_jobs.py`)
- **Purpose**: Data conformance and identity resolution
- **Process**:
  1. Apply identity resolution using crosswalks
  2. Implement SCD2 for reference data
  3. Handle late-arriving updates
  4. Apply data quality checks

#### Gold Jobs (`gold_jobs.py`)
- **Purpose**: Curated data and KPI aggregation
- **Process**:
  1. Apply publish gate evaluation
  2. Create business views and KPIs
  3. Optimize for query performance
  4. Apply access controls

### 4. SQL Layer (`src/pipelines/sql/`)

#### Unified View (`unified_view.sql`)
- **Purpose**: Single query interface for cross-domain data
- **Features**:
  - Deterministic joins across supplier, logistics, inventory
  - Zero orphan key guarantee
  - Performance optimized with proper indexing

#### KPI Queries (`kpis.sql`)
- **Purpose**: Pre-calculated business metrics
- **Metrics**:
  - Lead time calculations
  - Capacity utilization
  - Inventory sufficiency
  - Supplier performance

#### Freshness Functions (`freshness_fn.sql`)
- **Purpose**: SQL functions for freshness monitoring
- **Features**:
  - Real-time freshness status
  - SLA breach detection
  - Historical trend analysis

## Data Flow Architecture

### 1. Ingestion Flow
```
Source System → Schema Validation → Bronze Storage → Metadata Tracking
```

### 2. Conformance Flow
```
Bronze Data → Identity Resolution → Data Conformance → Silver Storage → Quality Checks
```

### 3. Publication Flow
```
Silver Data → Publish Gate → Quality Validation → Gold Storage → Access Controls
```

### 4. Monitoring Flow
```
Data Updates → Freshness Tracking → SLA Evaluation → Alert Generation → Dashboard Updates
```

## Technology Stack

### Core Technologies
- **Runtime**: Python 3.11, Databricks Runtime (DBR 15.x)
- **Storage**: Delta Lake on Databricks
- **Processing**: PySpark, SQL (Databricks SQL)
- **Deployment**: dbx (Databricks CLI)

### Data Quality & Governance
- **Validation**: Custom schema validation + Spark SQL constraints
- **Lineage**: Unity Catalog lineage + MLflow metadata
- **Access Control**: Unity Catalog grants and masking
- **Monitoring**: Structured logging + Databricks alerts

### Performance Optimization
- **Partitioning**: By date and business key
- **Optimization**: OPTIMIZE/ZORDER on key columns
- **Caching**: Delta Lake caching for frequent queries
- **Indexing**: Proper column ordering for joins

## Data Models

### Entity Relationships
```
Supplier (SCD2) ←→ Shipment (1:many)
Product ←→ InventoryPosition (1:many)
Product ←→ CostReference (1:many, SCD2)
Location ←→ InventoryPosition (1:many)
```

### Key Entities
- **Supplier**: SCD2 with business key resolution
- **Shipment**: Logistics data with carrier relationships
- **InventoryPosition**: Daily snapshots with quantity tracking
- **Crosswalk**: Identity resolution mappings

## Security & Access Control

### Unity Catalog Integration
- **Catalogs**: `bronze`, `silver`, `gold`, `ops`
- **Schemas**: Domain-specific organization
- **Tables**: Delta Lake with ACID guarantees
- **Grants**: Role-based access control

### Role Hierarchy
- **Analyst**: Read access to gold layer
- **Engineer**: Read/write access to silver/gold
- **Auditor**: Read access to all layers + lineage
- **External Partner**: Limited read access to specific datasets

## Monitoring & Observability

### Structured Logging
- **Levels**: DEBUG, INFO, WARNING, ERROR
- **Context**: Correlation IDs, timestamps, source systems
- **Metrics**: Latency, throughput, DQ pass/fail rates

### Freshness Monitoring
- **Supplier**: Daily updates (24h SLA)
- **Logistics**: Hourly updates (1h SLA)
- **Inventory**: Near real-time (5min SLA)

### Data Quality Metrics
- **Completeness**: Non-null key percentages
- **Uniqueness**: Duplicate detection rates
- **Referential Integrity**: Orphan key counts
- **Freshness**: SLA compliance rates

## Performance Targets

### Query Performance
- **KPI Queries**: P95 latency ≤ 5 seconds
- **Unified Views**: Sub-second response for common queries
- **Freshness Checks**: Real-time status updates

### Data Processing
- **Ingestion**: Batch processing with hourly schedules
- **Conformance**: Near real-time for critical data
- **Publication**: Immediate after quality validation

## Deployment Architecture

### Environment Structure
```
Development → Staging → Production
     ↓           ↓         ↓
  Local Dev   Databricks  Databricks
  (dbx)       (Jobs)      (Jobs + Alerts)
```

### CI/CD Pipeline
- **Validation**: Schema and contract tests
- **Deployment**: dbx-based job deployment
- **Testing**: Integration tests with validation datasets
- **Monitoring**: Automated performance validation

## Scalability Considerations

### Horizontal Scaling
- **Spark Clusters**: Auto-scaling based on workload
- **Delta Lake**: Partition pruning and columnar storage
- **Unity Catalog**: Distributed metadata management

### Vertical Scaling
- **Memory**: Optimized for large dataset processing
- **Storage**: Delta Lake compression and optimization
- **Compute**: GPU acceleration for ML workloads (future)

## Future Enhancements

### Planned Features
- **ML Integration**: Predictive analytics for supply chain
- **Real-time Streaming**: Kafka integration for live updates
- **Advanced DQ**: Great Expectations integration
- **Data Mesh**: Multi-domain data product architecture

### Extension Points
- **New Data Sources**: Plugin architecture for additional systems
- **Custom Metrics**: User-defined KPI calculations
- **Advanced Lineage**: Graph-based dependency tracking
- **API Gateway**: Centralized API management

## Troubleshooting Guide

### Common Issues
1. **Identity Resolution Failures**: Check crosswalk mappings and trust levels
2. **Freshness Breaches**: Verify data source connectivity and processing schedules
3. **Quality Gate Failures**: Review DQ check configurations and data quality
4. **Performance Issues**: Check partitioning, optimization, and resource allocation

### Debugging Tools
- **Logs**: Structured logging with correlation IDs
- **Metrics**: Databricks metrics and custom dashboards
- **Lineage**: Unity Catalog lineage for impact analysis
- **Validation**: Schema validation reports and error details

## Conclusion

The Navigator Supply Chain Lakehouse architecture provides a robust, scalable foundation for unified supply chain analytics with strong governance, quality controls, and observability. The modular design allows for easy extension and customization while maintaining data integrity and performance standards.
