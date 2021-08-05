venv:
	python3 -m venv venv
	. ./venv/bin/activate
	pip install --upgrade pip setuptools wheel
	pip install -e .[test]

pylint:
	. ./venv/bin/activate
	pylint --rcfile .pylintrc --disable duplicate-code tap_postgres/

test:
	. ./venv/bin/activate
	pytest --cov=tap_postgres  --cov-fail-under=59 tests -v
