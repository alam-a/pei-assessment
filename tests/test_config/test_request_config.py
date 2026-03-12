from unittest.mock import patch

import pytest
from assessment.config.request_config import RequestConfig
import sys

def test_request_config():
    with patch.object(sys, "argv", ["x.py", "db=coke", "input_location=/data/sources/"]):
        config = RequestConfig()
        assert config.db == "coke"
        assert config.input_location == "/data/sources/"

    with patch.object(sys, "argv", ["", " db =coke ", "input_location=/data/sources/"]):
        config = RequestConfig()
        assert config.db == "coke"
        assert config.input_location == "/data/sources/"

    # non k=v format for arg should be ignored
    with patch.object(sys, "argv", ["", " db =coke ", "input_location=/data/sources/", "hello"]):
        config = RequestConfig()
        assert config.db == "coke"
        assert config.input_location == "/data/sources/"
        assert "db" in config.__dict__
        assert "hello" not in config.__dict__

@pytest.mark.xfail(reason="Missing required arguments, should fail with KeyError")
def test_request_config_with_missing_required_args():
    with patch.object(sys, "argv", ["x.py", "input_location=/data/sources/"]):
        RequestConfig()