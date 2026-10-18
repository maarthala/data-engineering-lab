# Databrick Local Simulator — convenience targets.
#
# The list of compose files lives once in .env (COMPOSE_FILE), which docker
# compose reads automatically — so every target below just calls `docker compose`
# and they all stay in sync (no copy-pasted -f lists).
#
# Common usage:
#   make init        # download required JARs/tarballs (first time only)
#   make up          # start the whole stack (live view: log left, services right); builds only missing images
#   make build       # rebuild the images (after changing a Dockerfile, or pulling such changes), then start
#   make status      # live dashboard: all containers' logs + state/CPU/memory/port per service (q quits)
#   make ps          # show running services
#   make logs        # tail logs for all services
#   make down        # bring the stack down — keeps volumes (all data) and images
#   make clean       # stack down first: delete all its data (volumes) — fresh install, images kept
#   make destroy     # remove everything the stack made: containers, volumes, network, images

COMPOSE = docker compose

.DEFAULT_GOAL := help

.PHONY: help init up build up-plain status down stop restart ps logs config clean destroy docs docs-serve polaris-seed

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

init: ## Download required JARs/tarballs (first time only)
	bash init.sh

up: ## Start the whole stack (builds only missing images) — live view: log left, each service right
	@if command -v python3 >/dev/null 2>&1; then python3 tools/up.py $(if $(BUILD),--build); \
	  else $(COMPOSE) up -d $(if $(BUILD),--build); fi

build: ## Rebuild the images, then start — after changing a Dockerfile (make up warns when one changed)
	@if command -v python3 >/dev/null 2>&1; then python3 tools/up.py --build; else $(COMPOSE) up -d --build; fi

up-plain: ## Start the whole stack, plain docker compose output (BUILD=1: rebuild images first)
	$(COMPOSE) up -d $(if $(BUILD),--build)

status: ## Live dashboard of the running stack: all logs left; state, CPU, memory, port per service right (q quits)
	@python3 tools/up.py --watch

down: ## Bring the stack down (members' Jupyter servers too) — keeps all data and images; make up starts it again
	@if command -v python3 >/dev/null 2>&1; then python3 tools/up.py --stop; else \
	  ids=$$(docker ps -aq --filter "name=^jupyter-"); \
	  if [ -n "$$ids" ]; then echo "Removing members' Jupyter servers..."; docker rm -f $$ids >/dev/null; fi; \
	  $(COMPOSE) down; fi

stop: down ## Same as make down

restart: down up ## make down, then make up — keeps all data

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

clean: ## With the stack down: delete ALL its data (volumes) — a fresh install; images are kept (asks first)
	@python3 tools/reset.py clean

destroy: ## Remove everything the stack made — containers, volumes, network, images (asks first)
	@python3 tools/reset.py destroy

