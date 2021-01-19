#!/bin/bash -i

# coverage run $(python -m unittest etl.tests.test_dpla)

export PYTHONPATH=$(pwd)
export RUNNING_UNITTESTS=1

# coverage run etl/tests/test_dpla.py
# coverage run etl/tests/test_cali.py
# coverage run etl/tests/test_pth.py
# coverage run etl/tests/test_si.py

pattern="*"
# pattern="si"
# pattern="pth"
# pattern="dpla"

coverage run -m unittest etl/tests/test*${pattern}*.py

# coverage run run_tests.py all

coverage html
start htmlcov/index.html > /dev/null 2>&1
if [ $? != 0 ]
then

    open htmlcov/index.html

fi


# REVIEW TODO collect statistics about each institutions - e.g., how much of each institution has
# dates available and what different date formats are present for each?