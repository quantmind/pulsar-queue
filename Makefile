.PHONY: clean test coverage


PYTHON ?= python
PIP ?= pip

clean:
	rm -fr dist/ *.egg-info *.eggs .eggs build/
	find . -name '__pycache__' | xargs rm -rf

test:
	flake8
	$(PYTHON) -W ignore setup.py test -q --sequential

coverage:
	$(PYTHON) -W ignore setup.py test --coverage -q
