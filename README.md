# ENTSOE ETL Pipeline

A production-grade Python ETL pipeline for extracting energy market data from the ENTSOE Transparency Platform API and loading it into PostgreSQL. **Optimized for Databricks Free Edition** with a Databricks-native notebook approach.

## 🎯 Overview

This ETL pipeline fetches historical and daily energy market data for multiple European countries from the ENTSOE Transparency Platform, including:
- **Balancing Reserves**: Primary, secondary, and tertiary reserve volumes and prices
- **Day-Ahead Prices**: Hourly electricity prices in EUR/MWh

The pipeline is built with **Databricks-native architecture**, modular design, comprehensive error handling, and is specifically optimized for Databricks Free Edition deployment.

## 🚀 Databricks-Native Features

### Key Features for Databricks:
- ✅ **Notebook-based ETL** - Separate notebooks for historical and daily processing
- ✅ **Automatic environment detection** - Detects Databricks runtime automatically
- ✅ **Databricks-friendly logging** - Simplified log format for Databricks notebooks
- ✅ **Environment variable handling** - Works with Databricks cluster environment variables
- ✅ **File path resolution** - Handles Databricks workspace file paths
- ✅ **Compatible dependencies** - Version ranges tested with Databricks Runtime 13.3 LTS
- ✅ **Job scheduling** - Ready for Databricks Jobs with cron scheduling
- ✅ **CI/CD integration** - GitHub Actions for automated deployment

## 📁 Project Structure

```
entsoe-etl-databricks/
├── src/                    # Core Python modules
│   ├── config.py          # Configuration management (Databricks-aware)
│   ├── entsoe_api.py      # ENTSOE API client
│   ├── transform.py       # Data transformation and cleaning
│   ├── postgres_writer.py # PostgreSQL database operations
│   ├── utils.py           # Utility functions and helpers
│   └── country_config.json # Country configurations
├── notebooks/             # Databricks notebooks
│   ├── historical_etl.ipynb  # Historical data processing
│   └── daily_etl.ipynb       # Daily data processing
├── dags/                  # Databricks job configurations
│   └── databricks_job_config.json
├── tests/                 # Unit tests
│   └── test_entsoe_api.py
├── .github/               # CI/CD workflows
│   └── workflows/
│       └── ci-cd.yml
├── schema.sql            # PostgreSQL table schema
├── main.py               # Local development script
├── databricks_init.py    # Databricks environment initialization
├── requirements.txt      # Standard Python dependencies
├── requirements-databricks.txt  # Databricks-compatible dependencies
├── env.example          # Environment variables template
├── README.md            # This file
└── .gitignore           # Git ignore patterns
```

## 🚀 Quick Start

### 1. Prerequisites

- **Databricks Free Edition** (Community Edition)
- **Python 3.10+** (or Databricks Runtime 13.3 LTS)
- **PostgreSQL database** (Supabase recommended)
- **ENTSOE API key** (free registration at [ENTSOE Transparency Platform](https://transparency.entsoe.eu))

### 2. Installation

#### Local Development:
```bash
# Clone the repository
git clone https://github.com/yourusername/entsoe-etl-databricks.git
cd entsoe-etl-databricks

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp env.example .env

# Edit .env with your configuration
nano .env
```

#### Databricks Deployment:
1. Import repository into Databricks Repos
2. Use `requirements-databricks.txt` for package installation
3. Set environment variables in cluster configuration
4. Run `databricks_init.py` to test environment

### 3. Configuration

Edit the `.env` file with your settings:

```bash
# Required: ENTSOE API key
ENTSOE_API_KEY=your_actual_api_key_here

# Required: PostgreSQL connection string
DATABASE_URL=postgresql://username:password@host:port/database

# Optional: Override defaults
COUNTRY_EIC=10Y1001A1001A82H  # Germany
COUNTRY_CODE=DE
LOG_LEVEL=INFO
```

### 4. Database Setup

Run the schema file to create the required tables:

```sql
-- Execute schema.sql in your PostgreSQL database
\i schema.sql
```

The pipeline will automatically create:
- `balancing_reserves`: Balancing reserve volumes and prices
- `day_ahead_prices`: Day-ahead electricity prices

## 📊 Usage

### Databricks Notebooks (Recommended)

#### Historical ETL (1 Jan 2024 to Yesterday)
```python
# In Databricks notebook
%run /Workspace/Repos/your-repo-name/notebooks/historical_etl
```

#### Daily ETL (Yesterday's Data)
```python
# In Databricks notebook
%run /Workspace/Repos/your-repo-name/notebooks/daily_etl
```

### Local Development

#### Daily ETL (Yesterday's Data)
```bash
# Run for yesterday (default)
python main.py --mode daily

# Run for specific date
python main.py --mode daily --date 2024-01-15

# Run for specific country
python main.py --mode daily --country FR
```

#### Historical ETL (Date Range)
```bash
# Run from 2024-01-01 to today
python main.py --mode historical

# Run for specific date range
python main.py --mode historical --start-date 2024-01-01 --end-date 2024-01-31

# Run for specific country
python main.py --mode historical --country DE
```

### Command Line Options

```bash
python main.py --help
```

Options:
- `--mode`: `daily` or `historical` (default: daily)
- `--date`: Specific date in YYYY-MM-DD format or "daily"
- `--start-date`: Start date for historical mode
- `--end-date`: End date for historical mode (default: today)
- `--country`: Country code (e.g., DE, FR, IT). Defaults to DE

## 🏗️ Architecture

### Core Components

1. **ENTSOEAPIClient** (`src/entsoe_api.py`)
   - Handles API requests with retry logic
   - Parses XML responses into pandas DataFrames
   - Supports multiple countries via configuration
   - Supports balancing reserves and day-ahead prices

2. **DataTransformer** (`src/transform.py`)
   - Normalizes timestamps to UTC
   - Handles DST conversion
   - Removes invalid records and duplicates
   - Validates data quality

3. **PostgresWriter** (`src/postgres_writer.py`)
   - Manages PostgreSQL connections
   - Creates tables and indexes
   - Handles upsert operations
   - Provides database statistics

4. **Databricks Notebooks** (`notebooks/`)
   - **historical_etl.ipynb**: Processes historical data in batches
   - **daily_etl.ipynb**: Processes daily data for job scheduling

### Data Flow

```
ENTSOE API → Extract → Transform → Load → PostgreSQL
     ↓           ↓         ↓        ↓         ↓
  XML Data   DataFrames  Cleaned   Upsert   Tables
```

## 🧪 Testing

Run the unit tests:

```bash
python -m pytest tests/ -v
```

### Databricks Environment Testing

```bash
# Test Databricks environment setup
python databricks_init.py
```

## 🚀 Databricks Deployment

### 1. Setup Databricks Workspace

1. Create a free Databricks account at [databricks.com](https://databricks.com)
2. Create a new workspace
3. Set up a cluster (use the free tier)

### 2. Import Repository

1. In Databricks, go to **Repos**
2. Click **Add Repo**
3. Enter your GitHub repository URL
4. Select the branch (usually `main`)

### 3. Install Dependencies

Create a notebook to install dependencies:

```python
# Install Databricks-compatible packages
%pip install -r requirements-databricks.txt
```

### 4. Configure Environment Variables

In Databricks, go to **Compute** → **Clusters** → **Edit** → **Advanced Options** → **Environment Variables**:

```
ENTSOE_API_KEY=your_api_key
DATABASE_URL=your_postgresql_url
COUNTRY_EIC=10Y1001A1001A82H
COUNTRY_CODE=DE
LOG_LEVEL=INFO
```

### 5. Initialize Environment

```python
# Run the Databricks initialization script
%run /Workspace/Repos/your-repo-name/databricks_init.py
```

### 6. Run ETL Pipeline

#### Historical ETL (One-time)
```python
# Run historical ETL notebook
%run /Workspace/Repos/your-repo-name/notebooks/historical_etl
```

#### Daily ETL (Scheduled)
```python
# Run daily ETL notebook
%run /Workspace/Repos/your-repo-name/notebooks/daily_etl
```

### 7. Schedule Jobs

1. Go to **Workflows** → **Jobs**
2. Create a new job
3. Add a notebook task pointing to `notebooks/daily_etl`
4. Set up a schedule (e.g., daily at 4:00 UTC)
5. Use the provided job configuration in `dags/databricks_job_config.json`

## 📈 Database Schema

### balancing_reserves

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| country_code | VARCHAR(10) | Country code (DE, FR, etc.) |
| datetime_utc | TIMESTAMP WITH TIME ZONE | UTC timestamp |
| reserve_type | TEXT | Reserve type (Primary, Secondary, Tertiary) |
| amount_mw | FLOAT | Amount in MW |
| price_eur | FLOAT | Price in EUR (nullable) |
| inserted_at | TIMESTAMP WITH TIME ZONE | Record creation time |

### day_ahead_prices

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| country_code | VARCHAR(10) | Country code (DE, FR, etc.) |
| datetime_utc | TIMESTAMP WITH TIME ZONE | UTC timestamp |
| price_eur_per_mwh | FLOAT | Price in EUR/MWh |
| inserted_at | TIMESTAMP WITH TIME ZONE | Record creation time |

## 🔧 Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ENTSOE_API_KEY` | Yes | - | ENTSOE API key |
| `DATABASE_URL` | Yes | - | PostgreSQL connection string |
| `COUNTRY_EIC` | No | `10Y1001A1001A82H` | Germany EIC code |
| `COUNTRY_CODE` | No | `DE` | Country code |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `MAX_RETRIES` | No | `3` | API retry attempts |
| `RETRY_DELAY` | No | `5` | Retry delay in seconds |
| `REQUEST_TIMEOUT` | No | `30` | API request timeout |

### Supported Countries

The pipeline supports multiple European countries:

- **Germany (DE)**: `10Y1001A1001A82H`
- **France (FR)**: `10YFR-RTE------C`
- **Italy (IT)**: `10YIT-GRTN-----B`
- **Spain (ES)**: `10YES-REE------0`
- **Netherlands (NL)**: `10YNL----------L`
- **Belgium (BE)**: `10YBE----------2`
- **Austria (AT)**: `10YAT-APG------L`
- **Switzerland (CH)**: `10YCH-SWISSGRIDZ`
- **Poland (PL)**: `10YPL-AREA-----S`
- **Czech Republic (CZ)**: `10YCZ-CEPS-----N`

## 🔄 CI/CD Pipeline

The project includes GitHub Actions for automated testing and deployment:

### Workflow Jobs:
1. **Validate**: Code linting and unit tests
2. **Security**: Security checks with Bandit and secret detection
3. **Databricks Deploy**: Automated deployment to Databricks

### Setup CI/CD:
1. Add repository secrets:
   - `DATABRICKS_HOST`: Your Databricks workspace URL
   - `DATABRICKS_TOKEN`: Your Databricks access token

2. Push to main branch to trigger deployment

## 🛠️ Development

### Adding New Countries

1. Update `src/country_config.json` with new country EIC codes
2. The pipeline automatically supports new countries

### Adding New Data Types

1. Add new document types in `src/entsoe_api.py`
2. Create corresponding transformation methods in `src/transform.py`
3. Add database table and loading logic in `src/postgres_writer.py`
4. Update notebooks for new data types

## 📝 Logging

The pipeline uses structured logging with the following levels:
- `DEBUG`: Detailed debugging information
- `INFO`: General information about pipeline progress
- `WARNING`: Non-critical issues
- `ERROR`: Errors that don't stop the pipeline
- `CRITICAL`: Critical errors that stop execution

**Databricks Optimization**: Simplified log format for better readability in Databricks notebooks.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the [ENTSOE API documentation](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
2. Review the logs for error details
3. Run `databricks_init.py` to diagnose environment issues
4. Open an issue on GitHub

## 🔗 Links

- [ENTSOE Transparency Platform](https://transparency.entsoe.eu)
- [ENTSOE API Documentation](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
- [Databricks Documentation](https://docs.databricks.com)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Supabase Documentation](https://supabase.com/docs) 