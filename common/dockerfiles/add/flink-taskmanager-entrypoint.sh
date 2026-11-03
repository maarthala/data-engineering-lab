#!/bin/sh
# Beam's Python worker pool in the background (it starts a worker per pipeline stage it's asked for),
# then the TaskManager as Flink's image starts it. A starting worker prints the pipeline's options —
# among them the member's storage key — so those lines never reach the log.
/opt/apache/beam/boot --worker_pool 2>&1 | grep --line-buffered -v -e 'pipeline_options:' -e 'secret_access_key' &
exec /docker-entrypoint.sh "$@"
