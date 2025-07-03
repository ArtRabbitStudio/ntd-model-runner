#!/usr/bin/env bash

# bail out immediately if anything errors
set -euo pipefail

function info () {
	# shellcheck disable=SC2086,SC2027
    echo -e '\E[37;44m'"\033[1m"$1"\033[0m"
    tput -T linux sgr0
}

# check for packaged pipenv install
if [[ -z $( which pipenv ) ]] ; then
	echo "=> error: please install 'pipenv' using your system package manager (homebrew/aptitude)" >&2
	exit 1
fi


# set up the python virtualenv
info "-> setting up python SCH-STH-TRA model runner ..."

# switch to the right branch of the epioncho model
SCH_MODEL_BRANCH="${SCH_MODEL_BRANCH:-master}"
TRACHOMA_MODEL_BRANCH="${TRACHOMA_MODEL_BRANCH:-master}"
POSTPROCESSING_REPO_BRANCH="${POSTPROCESSING_REPO_BRANCH:-main}"
sed \
	-e "s/_sch_model_branch_/${SCH_MODEL_BRANCH}/" \
	-e "s/_trachoma_model_branch_/${TRACHOMA_MODEL_BRANCH}/" \
	-e "s/_postprocessing_repo_branch_/${POSTPROCESSING_REPO_BRANCH}/" \
	< pyproject.toml.tpl > pyproject.toml

# clear out existing virtualenv
set +e
pipenv --rm 2>/dev/null
set -e
rm -rf Pipfile*
pipenv --python 3.10.9
pipenv run curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
pipenv run python get-pip.py --force-reinstall
pipenv run rm get-pip.py
pipenv run pip install -U setuptools==78.1.0
pipenv run pip install .
echo

info "-> checking pip install inside pipenv ..."
pipenv run python check-python-env.py

info "-> SCH-STH-TRA model and runner are built and ready to run."
