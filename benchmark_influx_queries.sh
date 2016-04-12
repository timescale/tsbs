#!/usr/bin/env bash
set -euf -o pipefail
count=${1}
measurement=${2}
field=${3}
start_date=${4}
end_date=${5}
interval=${6}

echo "${count} queries:"
time (for i in `seq ${count}`; do ./influx_query.sh ${measurement} ${field} ${start_date} ${end_date} ${interval} >/dev/null; done 2>/dev/null)
