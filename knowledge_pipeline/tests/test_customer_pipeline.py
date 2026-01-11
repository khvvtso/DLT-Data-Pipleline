
import pytest

from knowledge_pipeline.pipelines.customer_master import CustomerMasterPipeline


@pytest.fixture
def sample_customer_csv(tmp_path):
    csv_data = """customer_id,first_name,last_name,email,signup_date,status
1,John,Doe,john@example.com,2023-01-15,active
2,Jane,Smith,jane@example.com,2023-02-20,active"""

    csv_file = tmp_path / "customers.csv"
    csv_file.write_text(csv_data, encoding="utf-8")
    return csv_file


def test_customer_pipeline_extract(sample_customer_csv):
    pipeline = CustomerMasterPipeline(str(sample_customer_csv))
    data = pipeline.extract()

    assert len(data) == 2
    assert data[0]["first_name"] == "John"
    assert data[1]["email"] == "jane@example.com"


def test_customer_pipeline_transform(sample_customer_csv):
    pipeline = CustomerMasterPipeline(str(sample_customer_csv))
    raw_data = pipeline.extract()
    transformed = pipeline.transform(raw_data)

    assert len(transformed) == 2
    assert transformed[0]["name"] == "John Doe"
    assert transformed[0]["customer_status"] == "active"
    assert "metadata" in transformed[0]
    assert "data_quality" in transformed[0]["metadata"]