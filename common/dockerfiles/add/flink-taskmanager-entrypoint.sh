#!/bin/sh
# Beam's Python worker pool in the background (it starts a worker per pipeline stage it's asked for) —
# with the Python boot: /opt/apache/beam/boot is Beam's Java worker, which the TaskManager starts itself
# when a job has Java steps (ReadFromKafka). Then the TaskManager, as Flink's image starts it. A starting worker prints the pipeline's options —
# among them the member's storage key — so those lines never reach the log.
python -m apache_beam.runners.worker.worker_pool_main --service_port=50000 \
    --container_executable=/opt/apache/beam-python/boot 2>&1 | grep --line-buffered -v -e 'pipeline_options:' -e 'secret_access_key' &
# checkpoints / savepoints on the shared volume belong to the flink user (a fresh volume is root's)
mkdir -p /opt/flink/data/checkpoints /opt/flink/data/savepoints 2>/dev/null \
  && chown flink:flink /opt/flink/data /opt/flink/data/checkpoints /opt/flink/data/savepoints 2>/dev/null
exec /docker-entrypoint.sh "$@"
