from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional
import dlt
from dlt.sources.helpers import requests
from datetime import datetime

class BasePipeline(ABC):
    """Base pipeline class that defines the interface for all data sources."""
    
    def __init__(self, source_name: str, destination: str = "duckdb"):
        project_root = Path(__file__).resolve().parents[2]
        self.pipeline = dlt.pipeline(
            pipeline_name=f"{source_name}_pipeline",
            destination=destination,
            dataset_name=source_name,
            pipelines_dir=str(project_root / ".dlt"),
        )
    
    @abstractmethod
    def extract(self) -> Any:
        """Extract data from the source."""
        pass
    
    @abstractmethod
    def transform(self, data: Any) -> List[Dict]:
        """Transform raw data into a structured format."""
        pass
    
    def load(self, data: List[Dict], table_name: str) -> None:
        """Load transformed data into the destination."""
        self.pipeline.run(
            data,
            table_name=table_name,
            write_disposition="merge",
            primary_key="id"
        )
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.utcnow().isoformat()
    
    def _is_late_arrival(self, record_time: str, threshold_days: int = 7) -> bool:
        """Check if a record is late-arriving based on its timestamp."""
        try:
            record_dt = datetime.fromisoformat(record_time)
            return (datetime.utcnow() - record_dt).days > threshold_days
        except (ValueError, TypeError):
            return False
    
    def run(self) -> None:
        """Run the complete ETL process."""
        print(f"Starting {self.__class__.__name__}...")
        raw_data = self.extract()
        print(f"Extracted {len(raw_data) if isinstance(raw_data, list) else 1} records")
        transformed_data = self.transform(raw_data)
        table_name = self.__class__.__name__.lower().replace("pipeline", "")
        self.load(transformed_data, table_name)
        print(f"Completed {self.__class__.__name__}")
