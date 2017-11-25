#!/usr/bin/env bash

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r requirements/hard.txt
pip install -r requirements/soft.txt
pip install -r requirements/dev.txt
