pylint:
	poetry run pylint --rcfile .pylintrc --disable duplicate-code tap_postgres/

test:
	poetry run pytest --cov=tap_postgres  --cov-fail-under=59 tests -v
