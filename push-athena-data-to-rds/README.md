# Push Athena Data to RDS

AWS Lambda function that queries Athena for billing metrics data and pushes it to the Badger RDS database.

## Overview

This Lambda processes data from multiple sources:
- `assettype` - AssetType/Cloudflare image bandwidth
- `host` - Primary domain host bandwidth and API requests
- `fastly_host` - Fastly CDN bandwidth and requests
- `varnish` - Varnish cache requests (sketches)
- `frontend_haproxy` - Frontend HAProxy requests
- `gumlet` - Gumlet image CDN bandwidth and requests

## Environment Variables

### Required Variables

| Variable | Description |
|----------|-------------|
| `BUCKET_NAME` | S3 bucket name for Athena query results |
| `S3_FILE_PATH` | S3 path prefix for storing results |
| `APP_HOST` | Badger application host URL |
| `APP_AUTH` | Authentication token for Badger API |
| `CLOUDFLARE_DB` | Athena database name for Cloudflare data |
| `ALB_DB` | Athena database name for ALB logs |
| `GUMLET_DB` | Athena database name for Gumlet data |
| `FASTLY_DB` | Athena database name for Fastly data |
| `ASSETTYPE_TABLE` | Athena table name for AssetType data |
| `PRIMARY_DOMAIN_TABLE` | Athena table name for primary domain data |
| `VARNISH_TABLE` | Athena table name for Varnish logs |
| `HAPROXY_TABLE` | Athena table name for HAProxy logs |
| `GUMLET_TABLE` | Athena table name for Gumlet data |
| `FASTLY_PRIMARY_DOMAIN_TABLE` | Athena table name for Fastly data |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATE` | Override date in format `YYYY-MM-DD` | Yesterday's date |
| `DATA_SOURCES` | Comma-separated list of data sources to process | All sources |

## Usage Examples

### Process All Data Sources (Default)

No `DATA_SOURCES` variable needed. Lambda will process all 6 sources.

### Process Only AssetType

Set in Lambda environment variables:
```
DATA_SOURCES=assettype
```

### Process Multiple Specific Sources

```
DATA_SOURCES=assettype,host,varnish
```

### Process for a Specific Date

```
DATE=2025-11-02
DATA_SOURCES=assettype
```

### Testing Locally

```bash
# Set all required environment variables first
export BUCKET_NAME="your-bucket"
export S3_FILE_PATH="athena-results/"
export APP_HOST="https://badger.example.com"
export APP_AUTH="your-auth-token"
# ... set other required variables

# Process only assettype
export DATA_SOURCES=assettype
export DATE=2025-11-02
go run push-athena-data-to-rds.go
```

## Building

```bash
# Build for Lambda (Linux)
make push-athena-data-to-rds

# Create deployment package
make push-athena-data-to-rds.zip
```

## Deployment

1. Build the deployment package:
   ```bash
   make push-athena-data-to-rds.zip
   ```

2. Upload to AWS Lambda:
   ```bash
   aws lambda update-function-code \
     --function-name push-athena-data-to-rds \
     --zip-file fileb://push-athena-data-to-rds.zip
   ```

3. Set environment variables as needed:
   ```bash
   aws lambda update-function-configuration \
     --function-name push-athena-data-to-rds \
     --environment "Variables={DATA_SOURCES=assettype,DATE=2025-11-02,...}"
   ```

## How It Works

1. **Date Resolution**: Uses `DATE` env var if present, otherwise defaults to yesterday
2. **Source Selection**: Uses `DATA_SOURCES` env var if present, otherwise processes all sources
3. **For Each Data Source**:
   - Queries Athena with date-based partitions (year/month/day)
   - Saves results to S3 as CSV
   - Calls Badger API endpoint `/api/save-athena-data` with S3 key and data source
4. **Badger Processing**:
   - Downloads CSV from S3
   - Validates and transforms data
   - Inserts into respective daily_* tables

## Data Flow

```
Athena Tables → Lambda Query → S3 CSV → Lambda POST → Badger API → PostgreSQL
```

## Troubleshooting

### Lambda logs show "Wrong DataSource"
- Check that `DATA_SOURCES` value is valid (must be one of: assettype, host, fastly_host, varnish, frontend_haproxy, gumlet)
- Ensure no extra spaces in comma-separated list

### Lambda logs show "Invalid Date Entered"
- Check `DATE` format is exactly `YYYY-MM-DD`
- Ensure month and day have zero-padding (e.g., `2025-01-05` not `2025-1-5`)

### Lambda logs show "Enter value for missing variables"
- Check all required environment variables are set
- Review CloudWatch logs to see which variables are missing

### No data processed
- Check that Athena tables have data for the specified date
- Verify IAM role has permissions to query Athena and read/write S3
- Check CloudWatch logs for Athena query execution status
