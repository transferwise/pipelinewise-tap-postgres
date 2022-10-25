venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc tap_postgres/

start_db:
	docker-compose up -d

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_postgres --cov-fail-under=58 tests/unit -v

integration_test:
	. ./venv/bin/activate ;\
	. ./tests/integration/env ;\
	pytest --cov=tap_postgres  --cov-fail-under=63 tests/integration -v
