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
info "-> setting up python Epioncho model runner ..."

# clear out existing virtualenv
set +e
pipenv --rm 2>/dev/null
set -e
rm -rf Pipfile*
pipenv install --dev
pipenv run pip install .
echo

info "-> Epioncho model and runner are built and ready to run."
