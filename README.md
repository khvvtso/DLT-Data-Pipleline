# DLT Knowledge Engineering Pipeline

A DLT-based data pipeline that demonstrates explicit knowledge engineering with semantic conflict resolution, late-arriving data handling, and agent-ready outputs.


This project is designed to run fully locally with no external services required.

Prerequisites
	•	macOS / Linux
	•	Python 3.10+
	•	Git

Verify Python: python3 --version

🚀 Run Locally (End-to-End)
# 1. Clone the repository
git clone https://github.com/khvvtso/DLT-Data-Pipleline.git
cd DLT-Data-Pipleline

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Run the pipeline
python dlt_pipeline.py

✅ Expected Output
==================================================
Running Customer Master Pipeline
==================================================
Extracted X records
Completed CustomerMasterPipeline

==================================================
Running Transaction Pipeline
==================================================
Extracted Y records
Completed TransactionPipeline

Pipeline execution completed successfully!

🔍 Verify Local Results
python - <<EOF
import duckdb, glob
db = glob.glob(".dlt/**/*.duckdb", recursive=True)[0]
con = duckdb.connect(db)
print(con.execute("SHOW TABLES").fetchall())
EOF

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │    │                 │
│   Data Sources  │───▶│  Raw Layer     │───▶│  Staging Layer  │───▶│  Curated Layer  │
│  (CSV, API)   │    │  (Raw data)    │    │  (Cleaned data) │    │  (Business      │
│                 │    │                 │    │                 │    │   logic)        │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                           │                        │                        │
                           ▼                        ▼                        ▼
                   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                   │                 │    │                 │    │                 │
                   │  Provenance     │    │  Data Quality   │    │  Agent-Ready    │
                   │  Tracking       │    │  Metrics        │    │  Output         │
                   │                 │    │                 │    │                 │
                   └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Data Flow Stages

### 1. Raw Layer (Bronze)
- **Purpose**: Preserve source-faithful data with minimal transformation
- **Characteristics**:
  - Original field names and formats
  - Source metadata and extraction timestamps
  - Append-only storage for auditability

### 2. Staging Layer (Silver)
- **Purpose**: Clean, type-normalized, standardized data
- **Characteristics**:
  - Consistent naming conventions (snake_case)
  - Proper data types (dates, decimals, enums)
  - Basic data quality validation
  - Late-arrival detection flags

### 3. Curated Layer (Gold)
- **Purpose**: Business-ready knowledge with resolved semantics
- **Characteristics**:
  - Semantic conflict resolution
  - Business logic applied
  - Agent-friendly schema
  - Rich metadata for consumption

## Facts vs Events Modeling

### Facts (Slowly Changing Dimensions)
**Customer Master Data** represents business entities:
- `customer_id`: Unique business identifier
- `name`, `email`, `join_date`: Profile attributes
- `customer_status`: Current state (active/inactive)
- **Properties**:
  - Changes tracked over time
  - Single source of truth for profile data
  - Used for joins and reference data

### Events (Immutable Business Occurrences)
**Transaction Data** represents business events:
- `transaction_id`: Unique event identifier
- `customer_id`: Foreign key to customer fact
- `amount`, `currency`: Event measures
- `event_time`: When the event occurred
- `transaction_status`: Event outcome
- **Properties**:
  - Immutable once created
  - Time-series nature
  - Append-only storage

## Semantic Conflict Resolution

### 1. Naming Conflicts
- **Customer status** vs **Transaction status**
  - Renamed to `customer_status` and `transaction_status`
  - Prevents ambiguity in downstream queries

### 2. Data Type Conflicts
- **Email casing**: Standardized to lowercase
- **Status values**: Normalized to lowercase enums
- **Currency**: Uppercase standardization
- **Dates**: ISO format consistency

### 3. Identity Conflicts
- **Customer ID formats**: CSV uses `1,2,3` vs API uses `cust_001, cust_002`
  - Curated layer normalizes to `cust_{id}` format
  - Enables reliable joins across sources

### 4. Business Logic Conflicts
- **Source authority**: Customer master is authoritative for profile data
- **Event autonomy**: Transaction data stands alone for events
- **Cross-reference**: Customer ID links facts to events

## Late-Arriving Data Handling

### Detection Logic
```python
def _is_late_arrival(self, record_time: str, threshold_days: int = 7) -> bool:
    """Check if a record is late-arriving based on its timestamp."""
    try:
        record_dt = datetime.fromisoformat(record_time)
        return (datetime.utcnow() - record_dt).days > threshold_days
    except (ValueError, TypeError):
        return False
```

### Implementation
- **Threshold**: 7 days (configurable)
- **Flagging**: `metadata.is_late_arrival` boolean
- **Processing**: Late records processed through same pipeline
- **Downstream handling**: Agents can filter or weight accordingly

### Metadata Enrichment
```json
{
  "metadata": {
    "source": "transaction_api_mock",
    "extracted_at": "2025-01-12T00:15:30.123456",
    "is_late_arrival": true,
    "arrival_delay_days": 12
  }
}
```

## Template Reusability

### Base Pipeline Interface
```python
class BasePipeline(ABC):
    def __init__(self, source_name: str, destination: str = "duckdb"):
        # Common DLT setup
        
    @abstractmethod
    def extract(self) -> Any:
        # Source-specific extraction
        
    @abstractmethod
    def transform(self, data: Any) -> List[Dict]:
        # Source-specific transformation
```

### Adding New Data Sources
1. **Create pipeline class** inheriting from `BasePipeline`
2. **Implement `extract()`** method for data retrieval
3. **Implement `transform()`** method with:
   - Standardized field names
   - Type conversion
   - Metadata enrichment
   - Quality checks
4. **Add to main runner** in `dlt_pipeline.py`

### Example: Adding Product Data
```python
class ProductPipeline(BasePipeline):
    def __init__(self, api_url: str):
        super().__init__("products")
        self.api_url = api_url
        
    def extract(self):
        # API call or file read
        return products_data
        
    def transform(self, data):
        transformed = []
        for product in data:
            transformed.append({
                "id": f"prod_{product['product_id']}",
                "product_id": product["product_id"],
                "name": product["name"].lower(),
                "category": product["category"].lower(),
                "price": float(product["price"]),
                "metadata": {
                    "source": "product_api",
                    "extracted_at": self._get_current_timestamp(),
                    "data_quality": self._check_quality(product)
                }
            })
        return transformed
```

## Agent Consumption Guidelines

### Safe Consumption Patterns

#### 1. Schema Inspection
```python
# Always check schema first
schema = con.execute("DESCRIBE TABLE customer_master").fetchdf()
print(schema)
```

#### 2. Data Quality Checks
```python
# Verify quality before consumption
quality_check = """
SELECT 
    COUNT(*) as total_records,
    COUNT_IF(metadata->>'data_quality'->>'has_required_fields' = 'true') as valid_records,
    COUNT_IF(metadata->>'data_quality'->>'is_valid_email' = 'true') as valid_emails
FROM customer_master
"""
quality = con.execute(quality_check).fetchdf()
```

#### 3. Late-Arrival Handling
```python
# Filter or weight late-arriving data
recent_data = con.execute("""
    SELECT * FROM transactions 
    WHERE NOT metadata->>'is_late_arrival' = 'true'
""").fetchdf()

# Or include with weighting
weighted_data = con.execute("""
    SELECT *, 
           CASE 
             WHEN metadata->>'is_late_arrival' = 'true' THEN 0.8 
             ELSE 1.0 
           END as confidence_weight
    FROM transactions
""").fetchdf()
```

#### 4. Semantic Consistency
```python
# Use resolved field names
customer_join = con.execute("""
    SELECT 
        c.customer_id,
        c.name,
        c.email,
        t.transaction_id,
        t.amount,
        t.event_time
    FROM customer_master c
    JOIN transactions t ON c.customer_id = t.customer_id
    WHERE c.customer_status = 'active'
      AND t.transaction_status = 'completed'
""").fetchdf()
```

### Consumption Best Practices

1. **Always check metadata** for data quality flags
2. **Handle late-arriving data** explicitly
3. **Use canonical field names** from curated layer
4. **Validate relationships** using foreign keys
5. **Consider data freshness** via `extracted_at` timestamps
6. **Implement retry logic** for transient pipeline issues

## Pipeline Execution

### Running the Pipeline
```bash
# Activate virtual environment
source venv/bin/activate

# Run end-to-end pipeline
python dlt_pipeline.py
```

### Expected Output
```
==================================================
Running Customer Master Pipeline
==================================================
Starting CustomerMasterPipeline...
Extracted 5 records
Completed CustomerMasterPipeline

==================================================
Running Transaction Pipeline
==================================================
Starting TransactionPipeline...
Extracted 100 records
Completed TransactionPipeline

Pipeline execution completed successfully!
```

### Verification
```bash
# Check DLT state
ls -la .dlt/

# Find DuckDB file
find .dlt -name "*.duckdb"

# Inspect tables
python -c "
import duckdb, glob
db = glob.glob('.dlt/**/*.duckdb', recursive=True)[0]
con = duckdb.connect(db)
print('Tables:', con.execute('SHOW TABLES').fetchdf())
print('Customer count:', con.execute('SELECT COUNT(*) FROM customer_master').fetchone())
print('Transaction count:', con.execute('SELECT COUNT(*) FROM transactions').fetchone())
"
```

## Testing

### Running Tests
```bash
pytest -v --cov=knowledge_pipeline
```

### Test Coverage
- Base pipeline functionality
- Customer master extraction and transformation
- Transaction extraction and transformation
- Late-arrival detection logic
- Data quality validation

## Data Quality Metrics

### Customer Master Quality
- **Required fields**: customer_id, first_name, last_name, email, signup_date
- **Email validation**: Basic format checking
- **Status normalization**: Lowercase enum values

### Transaction Quality
- **Required fields**: transaction_id, customer_id, amount
- **Amount validation**: Positive values only
- **Late arrival detection**: Timestamp-based flagging

## Extensibility Features

### Configuration-Driven
- Environment variables for API endpoints
- Configurable late-arrival thresholds
- Destination flexibility (DuckDB, PostgreSQL, etc.)

### Monitoring Hooks
- Custom metrics collection
- Error handling and logging
- Performance tracking

### Schema Evolution
- Backward-compatible transformations
- Versioned schema support
- Migration utilities

## Technology Stack

- **DLT**: Data loading and transformation framework
- **DuckDB**: Analytical database for local development
- **Python**: Core pipeline language
- **Pytest**: Testing framework
- **CSV**: Customer data source format
- **Mock API**: Transaction data simulation

## Project Structure

```
knowledge_pipeline/
├── data/
│   ├── raw/                 # Source data
│   ├── staging/              # Cleaned data
│   └── curated/             # Business-ready data
├── pipelines/
│   ├── __init__.py
│   ├── base_pipeline.py      # Base template
│   ├── customer_master.py    # Customer pipeline
│   └── transactions.py     # Transaction pipeline
├── tests/
│   ├── __init__.py
│   ├── conftest.py          # Test configuration
│   ├── test_base_pipeline.py
│   ├── test_customer_pipeline.py
│   └── test_transaction_pipeline.py
├── utils/
│   ├── __init__.py
│   ├── data_quality.py      # Quality utilities
│   └── late_arrival.py      # Late arrival handling
├── requirements.txt
├── .env.example
└── README.md
```

This pipeline demonstrates knowledge engineering principles through explicit modeling, conflict resolution, and agent-ready outputs while maintaining extensibility and testability.
