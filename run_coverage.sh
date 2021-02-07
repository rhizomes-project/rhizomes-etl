#!/bin/bash -i

# coverage run $(python -m unittest etl.tests.test_dpla)

export PYTHONPATH=$(pwd)
export RUNNING_UNITTESTS=1


if [ $# -eq 1 ]
then

    pattern=$1

else

    pattern="*"

fi

# pattern="cali"
# pattern="dpla"
# pattern="pth"
# pattern="si"

coverage run -m unittest etl/tests/test*${pattern}*.py

if [ "${pattern}" = "*" ]
then

	coverage html
	start htmlcov/index.html > /dev/null 2>&1
	if [ $? != 0 ]
	then

	    open htmlcov/index.html

	fi

fi


# REVIEW TODO collect statistics about each institutions - e.g., how much of each institution has
# dates available and what different date formats are present for each?
