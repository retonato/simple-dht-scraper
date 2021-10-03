#!/bin/bash

# Fail early
set -e

echo "--Black--"
black --check --diff --line-length=79 src

echo "--Flake8--"
flake8 src

echo "--Isort--"
isort --check --diff src

echo "--Mypy--"
mypy --cache-dir=/dev/null --ignore-missing-imports src

echo "--Pylint--"
pylint --score=no src