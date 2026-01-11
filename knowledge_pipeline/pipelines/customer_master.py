
import csv
from pathlib import Path
from typing import Dict, List

from knowledge_pipeline.pipelines.base_pipeline import BasePipeline


class CustomerMasterPipeline(BasePipeline):
    def __init__(self, csv_path: str):
        super().__init__("customer_master")
        self.csv_path = Path(csv_path)
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Customer data file not found: {csv_path}")

    def extract(self) -> List[Dict]:
        with self.csv_path.open("r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def transform(self, data: List[Dict]) -> List[Dict]:
        transformed: List[Dict] = []
        for row in data:
            transformed.append(
                {
                    "id": f"cust_{row['customer_id']}",
                    "customer_id": str(row["customer_id"]),
                    "name": f"{row['first_name']} {row['last_name']}",
                    "email": str(row["email"]).lower(),
                    "join_date": row["signup_date"],
                    "customer_status": str(row.get("status", "active")).lower(),
                    "metadata": {
                        "source": "customer_master_csv",
                        "extracted_at": self._get_current_timestamp(),
                        "data_quality": {
                            "has_required_fields": all(
                                field in row
                                for field in [
                                    "customer_id",
                                    "first_name",
                                    "last_name",
                                    "email",
                                    "signup_date",
                                ]
                            ),
                            "is_valid_email": "@" in str(row.get("email", "")),
                        },
                    },
                }
            )
        return transformed