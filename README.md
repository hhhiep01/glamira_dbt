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
├── pyproject.toml      # Poetry dependencies
├── README.md          # Documentation
│
├── macros/
│   └── generate_surrogate_key.sql
│
├── models/
│   ├── staging/
│   │   ├── sources.yml              # Source definitions
│   │   └── stg_checkout_success.sql # Staging model
│   │
│   ├── core/
│   │   ├── schema.yml              # Core schema tests
│   │   ├── dim_location.sql        # Location dimension
│   │   ├── dim_product.sql        # Product dimension
│   │   ├── dim_store.sql          # Store dimension
│   │   ├── dim_device.sql         # Device dimension
│   │   └── dim_customer.sql       # Customer dimension
│   │
│   └── mart/
│       ├── schema.yml              # Mart schema tests
│       ├── dim_date.sql            # Date dimension
│       └── fact_sales_order.sql   # Sales fact table
│
└── tests/
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
