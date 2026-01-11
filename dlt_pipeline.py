
import os
from pathlib import Path

from dotenv import load_dotenv

from knowledge_pipeline.pipelines.customer_master import CustomerMasterPipeline
from knowledge_pipeline.pipelines.transactions import TransactionPipeline


def setup_data_directories() -> None:
    base_dir = Path(__file__).parent
    for subdir in ["raw", "staging", "curated"]:
        (base_dir / "data" / subdir).mkdir(parents=True, exist_ok=True)


def generate_sample_data() -> None:
    sample_path = Path(__file__).parent / "data" / "raw" / "customers.csv"
    if not sample_path.exists():
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        sample_path.write_text(
            """customer_id,first_name,last_name,email,signup_date,status
1,John,Doe,john.doe@example.com,2023-01-15,active
2,Jane,Smith,jane.smith@example.com,2023-02-20,ACTIVE
3,Robert,Johnson,robert.j@example.com,2023-03-10,inactive
4,Emily,Davis,emily.d@example.com,2023-04-05,Active
5,Michael,Brown,michael.b@example.com,2023-05-12,active
""",
            encoding="utf-8",
        )


def main() -> None:
    load_dotenv()

    setup_data_directories()
    generate_sample_data()

    print("\n" + "=" * 50)
    print("Running Customer Master Pipeline")
    print("=" * 50)
    customer_pipeline = CustomerMasterPipeline(
        csv_path=str(Path(__file__).parent / "data" / "raw" / "customers.csv")
    )
    customer_pipeline.run()

    print("\n" + "=" * 50)
    print("Running Transaction Pipeline")
    print("=" * 50)
    transaction_pipeline = TransactionPipeline(
        api_url=os.getenv("TRANSACTION_API_URL", "mock://transactions"),
        api_key=os.getenv("TRANSACTION_API_KEY"),
    )
    transaction_pipeline.run()

    print("\nPipeline execution completed successfully!")


if __name__ == "__main__":
    main()