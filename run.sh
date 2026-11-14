#!/bin/bash
#
# Run the image built by build.sh, with ./data as its data directory.
#
# * The port is published on loopback only. Set FS_PUBLISH_ADDRESS (for
#   example to 0.0.0.0) to expose it further, and see docs/Configuration.md
#   about putting the funding endpoints behind something that authenticates.
# * The container runs as you, so it can write ./data -- the dynamic config
#   file lives there -- without that directory being opened up to everyone.
# * Nothing else from the host is mounted. In particular not the Docker
#   socket: the service never uses it, and it would make code execution in
#   the container root on the host (CS-527).
# * `docker stop` waits FS_STOP_TIMEOUT seconds, 150 unless set, before it
#   kills the container. On SIGTERM the service waits up to its shutdown grace
#   for a funding in progress to finish (CS-719): fund_worst_case_seconds in
#   GET /status plus 5s, 140s with [mapi_lite] at the defaults. Docker's own
#   10s would cut that funding off. Raise it if the timeouts are raised.
set -euo pipefail

PUBLISH_ADDRESS="${FS_PUBLISH_ADDRESS:-127.0.0.1}"
STOP_TIMEOUT="${FS_STOP_TIMEOUT:-150}"

docker run -it \
    --stop-timeout "${STOP_TIMEOUT}" \
    --user "$(id -u):$(id -g)" \
    -p "${PUBLISH_ADDRESS}:8080:8080" \
    --mount type=bind,source="$(pwd)"/data,target=/app/bin/data \
    --rm financing-service-rust \
    "$@"
