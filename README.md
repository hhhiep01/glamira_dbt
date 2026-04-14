# Glamira dbt Project

Data transformation project using dbt for BigQuery with star schema design.

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

## Setup & Usage

```bash
# Install dependencies
poetry install

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
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
