#!/usr/bin/env sh
set -euf -o pipefail
count=${1}
measurement=${2}
field=${3}
echo "${count} queries:"
time (for i in `seq ${count}`; do ./influx_query.sh ${measurement} ${field} > /dev/null; done 2>/dev/null)
