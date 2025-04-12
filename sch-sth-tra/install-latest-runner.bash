#!/usr/bin/env bash
export PYENV_ROOT="$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
cd
cd ntd-model-runner/sch-sth-tra
git reset --hard HEAD
git fetch
git pull origin feature/endgame-result-importer
bash setup.bash
