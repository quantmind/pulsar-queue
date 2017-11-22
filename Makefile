.PHONY: clean test testinstalled coverage wheels macwheels


PYTHON ?= python
PIP ?= pip

clean:
	rm -fr dist/ *.egg-info *.eggs .eggs build/
	find . -name '__pycache__' | xargs rm -rf

test:
	flake8
	$(PYTHON) -W ignore setup.py test -q --io uv

testinstalled:
	$(PYTHON) -W ignore runtests.py

coverage:
	export PULSARPY=yes; $(PYTHON) -W ignore setup.py test --coverage -q

wheels:
	rm -rf wheelhouse
	$(PYTHON) -m ci.build_wheels --pyversions 3.5 3.6

macwheels:
	export PYMODULE=pulsar; export WHEEL=macosx; export CI=true; ./ci/build-wheels.sh
