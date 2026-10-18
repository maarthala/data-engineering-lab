#!/usr/bin/env bash
# Bring the stack down — keeps volumes (all data) and images. Same as `make down`.
# The list of compose files lives once in .env (COMPOSE_FILE). Run from local/.
set -e
ids=$(docker ps -aq --filter "name=^jupyter-")          # members' Jupyter servers (JupyterHub's)
[ -n "$ids" ] && docker rm -f $ids >/dev/null
docker compose down
