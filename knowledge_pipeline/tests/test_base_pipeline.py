
from datetime import datetime, timedelta

import pytest

from knowledge_pipeline.pipelines.base_pipeline import BasePipeline


class TestBasePipeline:
    class TestPipeline(BasePipeline):
        def extract(self):
            return [{"id": "1", "data": "test"}]

        def transform(self, data):
            return [{"id": d["id"], "transformed": True} for d in data]

    def test_pipeline_initialization(self):
        pipeline = self.TestPipeline("test_source")
        assert pipeline is not None

    def test_get_current_timestamp(self):
        pipeline = self.TestPipeline("test_source")
        timestamp = pipeline._get_current_timestamp()
        assert isinstance(timestamp, str)
        assert "T" in timestamp

    def test_is_late_arrival(self):
        pipeline = self.TestPipeline("test_source")
        now = datetime.utcnow()

        recent = (now - timedelta(days=3)).isoformat()
        assert not pipeline._is_late_arrival(recent)

        late = (now - timedelta(days=10)).isoformat()
        assert pipeline._is_late_arrival(late)

    def test_run_flow(self, mocker):
        pipeline = self.TestPipeline("test_source")
        mock_load = mocker.patch.object(pipeline, "load")

        pipeline.run()

        assert mock_load.called
        args, _kwargs = mock_load.call_args
        assert len(args[0]) == 1
        assert args[0][0]["transformed"] is True