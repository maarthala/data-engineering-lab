# Databrick Local Simulator — convenience targets.
#
# The list of compose files lives once in .env (COMPOSE_FILE), which docker
# compose reads automatically — so every target below just calls `docker compose`
# and they all stay in sync (no copy-pasted -f lists).
#
# Common usage:
#   make init        # download required JARs/tarballs (first time only)
#   make up          # build + start the whole stack
#   make ps          # show running services
#   make logs        # tail logs for all services
#   make down        # stop the stack and remove volumes
#   make clean       # remove images built for this stack

COMPOSE = docker compose

.DEFAULT_GOAL := help

.PHONY: help init up down restart ps logs config clean docs docs-serve polaris-seed

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

init: ## Download required JARs/tarballs (first time only)
	bash init.sh

up: ## Build and start the whole stack
	$(COMPOSE) up -d --build

down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

restart: down up ## Restart the whole stack

ps: ## Show running services
	$(COMPOSE) ps

logs: ## Tail logs for all services (use `make logs S=kafka` for one)
	$(COMPOSE) logs -f $(S)

docs: ## Validate the course (mkdocs build --strict — same gate as the GitHub Pages CI)
	docker run --rm -v "$(CURDIR)/../training":/docs squidfunk/mkdocs-material:9 build --strict
	@echo "✅ course builds strict-clean. It publishes to GitHub Pages on push to main (see .github/workflows/docs.yml)."

docs-serve: ## Live-preview the course while authoring (hot-reload) at http://localhost:8010
	@echo "Serving course at http://localhost:8010 (Ctrl-C to stop) ..."
	docker run --rm -it -p 8010:8000 -v "$(CURDIR)/../training":/docs squidfunk/mkdocs-material:9 serve -a 0.0.0.0:8000

polaris-seed: ## Provision the governed Polaris catalog + personas/RBAC (the governance unit, live)
	bash ../common/polaris/seed-polaris.sh

config: ## Validate the merged compose configuration
	$(COMPOSE) config -q && echo "compose config OK"

clean: ## Remove images built for this stack
	bash remove-stack.sh

