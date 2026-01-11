
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from knowledge_pipeline.pipelines.base_pipeline import BasePipeline


class TransactionPipeline(BasePipeline):
    def __init__(self, api_url: str, api_key: Optional[str] = None):
        super().__init__("transactions")
        self.api_url = api_url
        self.api_key = api_key

    def extract(self) -> List[Dict]:
        base_date = datetime.utcnow() - timedelta(days=60)
        transactions: List[Dict] = []

        for i in range(1, 101):
            tx_date = (base_date + timedelta(days=random.randint(0, 60))).isoformat()
            transactions.append(
                {
                    "transaction_id": f"txn_{i:05d}",
                    "customer_id": f"cust_{random.randint(1, 100):03d}",
                    "amount": round(random.uniform(10, 1000), 2),
                    "currency": "USD",
                    "transaction_date": tx_date,
                    "status": random.choice(["completed", "pending", "failed"]),
                    "category": random.choice(
                        ["electronics", "groceries", "utilities", "entertainment"]
                    ),
                }
            )

        return transactions

    def transform(self, data: List[Dict]) -> List[Dict]:
        transformed: List[Dict] = []
        for tx in data:
            event_time = tx["transaction_date"]
            is_late = self._is_late_arrival(event_time)
            transformed.append(
                {
                    "id": tx["transaction_id"],
                    "transaction_id": tx["transaction_id"],
                    "customer_id": tx["customer_id"],
                    "amount": float(tx["amount"]),
                    "currency": str(tx["currency"]).upper(),
                    "event_time": event_time,
                    "transaction_status": str(tx["status"]).lower(),
                    "category": str(tx["category"]).lower(),
                    "metadata": {
                        "source": "transaction_api_mock",
                        "extracted_at": self._get_current_timestamp(),
                        "is_late_arrival": is_late,
                    },
                }
            )

        return transformed