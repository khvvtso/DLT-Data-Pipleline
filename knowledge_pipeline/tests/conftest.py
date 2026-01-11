
from pathlib import Path
import shutil

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment."""
    test_data_dir = Path("tests/data")
    test_data_dir.mkdir(parents=True, exist_ok=True)
    yield
    # Clean up after tests
    if test_data_dir.exists():
        shutil.rmtree(test_data_dir)