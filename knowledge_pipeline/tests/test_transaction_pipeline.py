
from datetime import datetime, timedelta

from knowledge_pipeline.pipelines.transactions import TransactionPipeline


def test_transaction_pipeline_extract_has_rows():
    pipeline = TransactionPipeline("mock://transactions")
    data = pipeline.extract()
    assert len(data) == 101 - 1 + 1


def test_transaction_pipeline_transform_sets_late_arrival():
    pipeline = TransactionPipeline("mock://transactions")
    now = datetime.utcnow()
    data = [
        {
            "transaction_id": "txn_00001",
            "customer_id": "cust_001",
            "amount": 10.0,
            "currency": "USD",
            "transaction_date": (now - timedelta(days=10)).isoformat(),
            "status": "completed",
            "category": "groceries",
        }
    ]
    transformed = pipeline.transform(data)
    assert transformed[0]["metadata"]["is_late_arrival"] is True