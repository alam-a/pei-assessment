# pei-assessment
Boiler plate generated using UV.

To setup the environment, run this (uv is a pre-requisite)
```bash
uv sync
```

To run the code, run this
```bash
uv run main.py # or alternatively, python main.py if you have activated the virtual environment
```

To run the tests, use this
```bash
python -m pytest
python -m pytest -vv # for verbose test output
python -m pytest --cov=assessment --cov-report=html # for coverage report
```

/assessment contains the source code for the assessment
/data contains the test data for the assessment shared by pei
