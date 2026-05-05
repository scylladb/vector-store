#!/bin/bash
# Common utilities for release scripts.
# Source this file: source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

# Colors (disabled if not a terminal).
if [[ -t 1 ]]; then
    bold=$'\033[1m'
    red=$'\033[0;31m'
    green=$'\033[0;32m'
    cyan=$'\033[0;36m'
    reset=$'\033[0m'
else
    bold="" red="" green="" cyan="" reset=""
fi

# Semantic output helpers.
info()    { echo "  $*"; }
success() { echo "  ${green}$*${reset}"; }
error()   { echo "${red}error: $*${reset}" >&2; }

# Step counter — caller must set $total_steps before first call.
_step=0

step_header() {
    _step=$((_step + 1))
    local prefix=${STEP_PREFIX:-}
    echo ""
    echo "  ${bold}${cyan}--> [${prefix}${_step}/${total_steps}] $1${reset}"
    echo ""
}
