# Data Pipeline Automation Project

## Setup Instructions

1. **Install Dependencies**
```bash
   pip install -r requirements.txt
```

2. **Setup Database**
```bash
   psql -U postgres -f init_database.sql
```

3. **Configure Environment**
```bash
   cp .env.example .env
   # Edit .env with your actual values
```

4. **Test Pipeline**
```bash
   python pipeline.py
```

5. **Start Airflow**
```bash
   airflow webserver --port 8080 &
   airflow scheduler &
```

## Project Structure
- `config.py` - Configuration management
- `extractors.py` - Data source connectors  
- `transformers.py` - Data processing logic
- `pipeline.py` - Main pipeline orchestration
- `airflow_dag.py` - Workflow definition

## Testing
Run individual components to test your implementation before full pipeline execution.