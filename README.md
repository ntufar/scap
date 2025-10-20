# Navigator Supply Chain Lakehouse

A Databricks-based, multi-source supply chain data platform that unifies supplier, logistics, and inventory data into a conformed analytics layer with deterministic identity resolution and strong governance/observability.

## 🎯 Project Overview

This project implements a **medallion architecture** data lakehouse for supply chain analytics, providing:

- **Unified Data Layer**: Deterministic joins across supplier, logistics, and inventory domains
- **Identity Resolution**: Crosswalk-based key mapping with SCD2 history tracking
- **Data Quality Gates**: Publication blocking on critical DQ failures
- **Role-Based Security**: Granular access controls via Unity Catalog
- **Observability**: Structured logging, metrics, and freshness monitoring

## 🏗️ Architecture

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion from source systems
- **Silver Layer**: Conformed data with business logic and SCD2
- **Gold Layer**: Curated analytics views and KPIs

### Technology Stack
- **Platform**: Databricks with Unity Catalog
- **Storage**: Delta Lake tables
- **Processing**: PySpark/SQL pipelines
- **Deployment**: dbx (Databricks CLI)
- **Language**: Python 3.11

## 📁 Project Structure

```
├── src/                           # Source code
│   ├── cli/                       # Command-line interface
│   ├── lib/                       # Utilities (logging, DQ, lineage)
│   └── pipelines/                 # Data processing jobs
│       ├── sql/                   # SQL scripts
│       ├── bronze_jobs.py         # Raw data ingestion
│       ├── silver_jobs.py         # Data conformance
│       └── gold_jobs.py           # Analytics curation
├── tests/                         # Test files
├── docs/                          # Documentation
├── specs/master/                  # Project specifications
│   ├── checklists/               # Quality checklists
│   ├── contracts/                # API specifications
│   ├── data-model.md             # Entity relationships
│   ├── plan.md                   # Technical implementation plan
│   ├── quickstart.md             # Getting started guide
│   ├── research.md               # Technical decisions
│   ├── spec.md                   # Feature specification
│   └── tasks.md                  # Implementation tasks
├── .dbx/                         # Databricks deployment config
└── requirements.txt              # Python dependencies
```

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Databricks workspace with Unity Catalog
- Java 8 or 11 (for local PySpark development)

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd scap

# Install dependencies
pip install -r requirements.txt

# Configure Databricks CLI
databricks configure --token
```

### Setup Unity Catalog
```bash
# Validate SQL syntax (local)
python -m src.pipelines.validate_uc

# Deploy to Databricks (requires workspace access)
python -m src.pipelines.setup_uc
```

### Run Pipelines
```bash
# Load sample data
python -m src.cli load_sample_data --domain supplier

# Run pipeline stages
python -m src.cli run_pipeline --stage bronze
python -m src.cli run_pipeline --stage silver
python -m src.cli run_pipeline --stage gold
```

## 📚 Documentation

### Core Documentation
- **[Project Specification](specs/master/spec.md)** - Complete feature requirements
- **[Technical Plan](specs/master/plan.md)** - Architecture and implementation details
- **[Data Model](specs/master/data-model.md)** - Entity relationships and validation rules
- **[Quickstart Guide](specs/master/quickstart.md)** - Step-by-step setup instructions

### Implementation Artifacts
- **[Task List](specs/master/tasks.md)** - Complete implementation roadmap
- **[API Contracts](specs/master/contracts/openapi.yaml)** - REST API specifications
- **[Research Notes](specs/master/research.md)** - Technical decisions and rationale

### Quality Assurance
- **[Requirements Checklist](specs/master/checklists/requirements.md)** - Specification quality validation
- **[API Checklist](specs/master/checklists/api.md)** - API requirements validation

### Deployment & Operations
- **[Deployment Guide](docs/deployment_guide.md)** - Unity Catalog setup and deployment
- **[Unity Catalog Bootstrap](src/pipelines/sql/bootstrap_uc.sql)** - Catalog and schema creation
- **[Table Definitions](src/pipelines/sql/create_tables.sql)** - All table schemas

## 🔧 Development

### Project Setup
1. **Environment**: Python 3.11 with virtual environment
2. **Dependencies**: Install from `requirements.txt`
3. **Configuration**: Set up Databricks CLI authentication
4. **Validation**: Run SQL validation before deployment

### Code Structure
- **`src/lib/`**: Reusable utilities (logging, DQ, lineage)
- **`src/pipelines/`**: Data processing jobs by layer
- **`src/cli/`**: Command-line interface for local development
- **`tests/`**: Unit and integration tests

### Quality Gates
- **SQL Validation**: Syntax checking before deployment
- **Data Quality**: Publication blocking on critical failures
- **Freshness Monitoring**: SLA tracking per domain
- **Access Control**: Role-based permissions

## 📊 Data Domains

### Supplier Data
- **Source**: ERP systems, supplier portals
- **Freshness**: Daily updates
- **Key Features**: SCD2 history, identity resolution

### Logistics Data
- **Source**: Transportation management systems
- **Freshness**: Hourly updates
- **Key Features**: Route tracking, status monitoring

### Inventory Data
- **Source**: Warehouse management systems
- **Freshness**: Near real-time (≤5 minutes)
- **Key Features**: Position tracking, safety stock

## 🔐 Security & Governance

### Unity Catalog Security
- **Analysts**: Read access to gold layer
- **Engineers**: Full access to all layers
- **Auditors**: Read access for compliance
- **External Partners**: Limited access to specific views

### Data Quality
- **Critical Checks**: Block publication on failures
- **Monitoring**: Real-time DQ status tracking
- **Lineage**: Complete data lineage capture

## 📈 Performance Targets

- **KPI Queries**: P95 latency ≤ 5 seconds
- **Freshness SLAs**: 
  - Supplier: Daily
  - Logistics: Hourly
  - Inventory: ≤5 minutes

## 🛠️ Implementation Status

### ✅ Phase 1 - Setup (Complete)
- [x] T001: Project structure initialization
- [x] T002: Databricks CLI and dbx configuration
- [x] T003: Unity Catalog objects creation
- [x] T004: Python environment and dependencies
- [x] T005: Logging utility implementation
- [x] T006: Data quality helper implementation

### 🚧 Phase 2 - Foundational (In Progress)
- [ ] T007: Crosswalk model implementation
- [ ] T008: Identity resolution functions
- [ ] T009: Freshness status table and updater
- [ ] T010: Publish gate implementation

### 📋 Upcoming Phases
- **Phase 3**: User Story 1 - Unified Supply Chain Data
- **Phase 4**: User Story 2 - Supply Chain KPI Insights
- **Phase 5**: User Story 3 - Governance & Observability

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch
3. **Follow** the coding standards
4. **Run** validation scripts
5. **Submit** a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Documentation**: Check the `docs/` directory
- **Issues**: Create GitHub issues for bugs or feature requests
- **Questions**: Refer to the quickstart guide and technical plan

## 🔗 Related Links

- [Databricks Documentation](https://docs.databricks.com/)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [dbx CLI Documentation](https://docs.databricks.com/dev-tools/dbx/)

---

**Last Updated**: October 2024  
**Version**: 0.1.0  
**Status**: Development Phase
