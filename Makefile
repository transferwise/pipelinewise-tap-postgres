pylint:
	pylint --rcfile .pylintrc tap_postgres

test:
	pytest --cov=tap_postgres tests -v
