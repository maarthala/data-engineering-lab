# Bring up the stack

This part is for **whoever runs the platform**: you on your laptop (Docker Compose) or an
operator on a cluster (Kubernetes). If someone already runs the stack for you and gave you URLs,
you don't need it: go straight to [Prerequisites](../prerequisites.md) (just a browser) and start
learning.

!!! info "Two ways to run the *same* stack"
    The lakehouse is identical either way: same services, same course. **Docker Compose** runs it
    on one machine (the normal choice for learning). **Kubernetes** runs it on a cluster (closer
    to production).

## On your laptop, in four steps

| Step | Page | When |
|---|---|---|
| 1 | [0.2.1 First run: the mandatory make commands](first-run.md) | once: get the repo, create `.env`, `make init` |
| 2 | [0.2.2 make status: how to use it](make-status.md) | **recommended**: start Core + only what you're learning (6–10 GB for Docker) |
| 2′ | [0.2.3 Full stack: make up](full-stack.md) | instead of step 2, when you have 12–16 GB for Docker |
| 3 | [0.2.4 General information](general.md) | first time: check it works and fill the shared lake once; then the day-to-day commands |

!!! tip "Start with `make status`, not `make up`"
    `make up` starts **every** service, about 9 GB before you run anything. On a laptop that
    gives Docker less than 12 GB, everything gets slow and containers are killed. `make status`
    lets you start Core and only the units the lesson needs.

## On Kubernetes

An operator task: see [General information → On Kubernetes](general.md#on-kubernetes).

## You can now…
- Bring the stack up with Docker Compose (`cp .env.example .env`, `make init`, then `make status` for only the units you're learning or `make up` for everything, then the `shopflow_medallion` DAG once) or on Kubernetes (Ansible)
- Verify every service is running and reach the landing page
- Tear it down / reset cleanly, and know which command wipes data
