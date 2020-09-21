pylint:
	pylint --rcfile .pylintrc --disable duplicate-code tap_postgres/

test:
	pytest --cov=tap_postgres  --cov-fail-under=59 tests -v
