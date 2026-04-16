# Glamira dbt Project

Data transformation project using dbt for BigQuery with star schema design.

## Dashboards

### 01. Revenue Analysis
Revenue by month, country, and product.
🔗 [Open Dashboard](https://datastudio.google.com/s/msfztTeL6cM)

### 02. Geographic Distribution
Orders by country, top cities, and regions.
🔗 [Open Dashboard](https://datastudio.google.com/s/mpCrYcSUw_M)

### 03. Time-based Trends
Revenue trends by month, day of week, quarter, and hour.
🔗 [Open Dashboard](https://datastudio.google.com/s/mWw99xulpU4)

### 04. Product Performance
Top products, device types, browsers, and OS.
🔗 [Open Dashboard](https://datastudio.google.com/s/tEJWLyfbumI)

## Project Structure

```
dbt_project/
├── dbt_project.yml      # Project configuration
├── profiles.yml         # BigQuery connection
├── pyproject.toml       # Poetry dependencies
├── README.md            # Documentation
│
├── macros/
│   └── generate_surrogate_key.sql
│
├── models/
│   ├── staging/
│   │   ├── schema.yml              # Staging model docs
│   │   └── stg_raw_events.sql       # Staging: parse raw events, unnest cart_products
│   │
│   ├── core/
│   │   ├── schema.yml              # Core schema docs + tests
│   │   ├── dim_customer.sql        # Customer dimension
│   │   ├── dim_product.sql         # Product dimension
│   │   ├── dim_location.sql        # Location dimension (IP → city/country)
│   │   ├── dim_device.sql          # Device dimension (user-agent → browser/OS)
│   │   └── dim_store.sql           # Store dimension
│   │
│   └── mart/
│       ├── schema.yml              # Mart schema docs + tests
│       ├── dim_date.sql            # Date dimension (2020-2030)
│       └── fact_sales_order.sql    # Sales fact table (incremental)
```

## Data Model (Star Schema)

```
dim_location ───────┐
dim_product ────────┼──▶ fact_sales_order
dim_store ──────────┤
dim_device ─────────┤
dim_customer ───────┤
dim_date ────────────┘
```

## BigQuery Architecture

Each layer = 1 BigQuery dataset (1 dataset per layer standard).

```
BigQuery (project: todo-459814)
├── glamira_raw/           ← Dataset: raw layer (source data)
│   ├── raw_events         ← Table: website events source
│   ├── ip_locations       ← Table: IP → location mapping
│   └── product_names_raw  ← Table: product_id → name mapping
│
├── glamira_staging/       ← Dataset: staging layer
│   └── stg_raw_events     ← View: parsed & cleaned events
│
├── glamira_core/          ← Dataset: core layer (dimensions)
│   ├── dim_customer       ← Table: customer dimension
│   ├── dim_product        ← Table: product dimension
│   ├── dim_location       ← Table: location dimension
│   ├── dim_device         ← Table: device dimension
│   └── dim_store          ← Table: store dimension
│
└── glamira_mart/          ← Dataset: mart layer (facts)
    ├── dim_date           ← Table: date dimension (static)
    └── fact_sales_order   ← Table: sales fact (incremental)
```

### Data Flow

```
glamira_raw         (source)
      │
      ▼
glamira_staging     (stg_raw_events → view)
      │
      ▼
glamira_core        (dim_* → incremental tables)
      │
      ▼
glamira_mart        (fact_sales_order → incremental)
```

### Materialization Strategy

| Layer | Materialization | Purpose |
|-------|----------------|---------|
| staging | view | Cost efficient, reads from raw |
| core | incremental | Dimension - append new records only |
| mart | table / incremental | Facts - incremental by order |

## Data Sources

| Source Table | Description |
|---|---|
| `glamira_raw.raw_events` | Main event data from website |
| `glamira_raw.ip_locations_raw` | IP to country/city mapping |
| `glamira_raw.product_names_raw` | Product ID to name mapping |

## Key Metrics

- **Total Revenue**: ~800M CHF
- **Total Orders**: ~26K orders
- **Top Country**: United Kingdom, Vietnam
- **Top Product**: Wedding Ring Noble Infinite 5mm

## Setup & Usage

```bash
# Install dependencies
poetry install

# Run models
poetry run dbt run

# Run tests
poetry run dbt test

# Generate documentation
poetry run dbt docs generate
poetry run dbt docs serve
```

## Naming Conventions

- **Dimension tables**: `dim_<entity>`
- **Fact tables**: `fact_<event>`
- **Primary keys**: `<entity>_key` (BIGINT)
- **Foreign keys**: same as dimension key
- **Natural keys**: `<entity>_id` (VARCHAR)
- **Attributes**: `*_name`, `*_description`
- **Booleans**: `is_*`, `has_*`
- **Dates**: `*_date`, `*_timestamp`
