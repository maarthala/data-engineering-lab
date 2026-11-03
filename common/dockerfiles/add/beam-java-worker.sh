#!/bin/sh
# Beam's Java worker (environment PROCESS — the TaskManager starts it for Java steps like ReadFromKafka).
# It writes the job's options to pipeline_options.json in its working folder and reads them from there:
# a folder of its own, readable only by this user (the options hold the member's keys), and its
# start-up dump of those options never reaches the console.
umask 077
dir=$(mktemp -d /tmp/beam-java-XXXXXX) && cd "$dir" || exit 1
/opt/apache/beam/boot "$@" 2>&1 | grep --line-buffered -v -e 'pipeline_options' -e 'Provision info'
