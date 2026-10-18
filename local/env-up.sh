#!/usr/bin/env bash
# Build and start the whole stack.
# The list of compose files lives once in .env (COMPOSE_FILE), so docker compose
# picks them all up automatically. Run this from the repo root.
set -e
docker compose up -d          # `make build` (or --build) rebuilds the images
