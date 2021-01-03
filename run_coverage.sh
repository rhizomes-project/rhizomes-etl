#!/bin/bash -i

# coverage run $(python -m unittest etl.tests.test_dpla)

export PYTHONPATH=$(pwd)
export RUNNING_UNITTESTS=1

coverage run etl/tests/test_dpla.py

# coverage run run_tests.py all

coverage html
start htmlcov/index.html > /dev/null 2>&1
if [ $? != 0 ]
then

    open htmlcov/index.html

fi
