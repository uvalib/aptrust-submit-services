# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Four Go worker services that make up the UVA APTrust submission pipeline. Each is a standalone `package main` binary with its own `go.mod`, built into a single shared Docker image (`package/Dockerfile`). Each one runs as its own ECS task, started by its entry script in `package/scripts/`.

| Service | Consumes event | Publishes |
|---|---|---|
| `aptrust-submit-validator` | `EventSubmissionValidate` | `EventSubmissionReconcile` / `EventSubmissionValidateFail` |
| `aptrust-submit-reconciler` | `EventSubmissionReconcile` | `EventSubmissionApprove` / `EventSubmissionReconcileFail` |
| `aptrust-submit-bagger` | `EventBagInitiate` | `EventBagBuilt` |
| `aptrust-submit-submitter` | `EventBagBuilt` | `EventBagSubmitted` |

Something outside this repo (a state manager lambda) turns an approved submission into `EventBagInitiate` events. Event types and payloads come from the external module `github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus`. Database access (Postgres) goes through `github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao`. To develop against local checkouts of either module, uncomment the `replace` lines in the service's `go.mod`.

## Shared code via symlinks

`service-common/*.go` holds code that every service shares: the `main()` SQS poll loop, env helpers, S3/SQS clients, event publishing, and version info. The services don't import it as a package. Instead, `make common` symlinks these files into each service directory so they compile as part of that service's `package main`. `.gitignore` ignores the symlinked copies, so **always edit the files in `service-common/`, never the symlinks**. A change there affects all four services.

## Runtime model (service-common/main-cmdline.go)

- `main()` long-polls the SQS queue named by `NOTIFY_IN_QUEUE`. It decodes the EventBridge envelope into a `uvaaptsbus.UvaBusEvent` and runs `worker(doneChan, cfg, ev)` in a goroutine. While the worker runs, it extends the message's visibility timeout every `NOTIFY_QUEUE_HEARTBEAT_TIME` seconds.
- Each service defines its own `worker()` in `worker.go`. The value the worker sends on `done` decides what happens to the message:
  - `done <- true`: delete the message. Use this on success, for events the service doesn't handle, **and for permanent failures** (for example, the validator records a failure in the DB, then signals true so the message isn't reprocessed).
  - `done <- false`: leave the message on the queue so it is retried after the visibility timeout. Use this for transient errors.
- Each service's `config.go` loads its configuration from environment variables. Missing required vars are fatal at startup. `EVENT_BUS_NAME` is optional; if it's empty, no events are published.

## Build commands

Run these inside a service directory (for example, `cd aptrust-submit-validator`):

```sh
make build     # symlink common files + darwin binary -> bin/<name>.darwin (built with -race)
make linux     # symlink common files + static linux binary -> bin/<name>.linux
make vet
make fmt
make check     # staticcheck (-checks all,-S1002,-ST1003) + shadow vet
make dep       # go get -u, go mod tidy, go mod verify
```

The Makefiles build with `*.go`, so run `make common` before you use `go build`/`go vet` directly. There are no tests in this repo.

Build the full container from the repo root: `docker build -f package/Dockerfile -t aptrust-submit-services .`

## CI/CD

`pipeline/buildspec.yml` (AWS CodeBuild) builds the image, pushes it to ECR, and writes the build tag to SSM at `/containers/$CONTAINER_IMAGE/latest`. `pipeline/deployspec.yml` clones `github.com/uvalib/terraform-infrastructure` and runs `terraform apply` for each of the four staging ECS tasks under `aptrust-submit/ecs-tasks/staging/`. If you add a new service, update the Dockerfile, add an entry script, and add a deployspec step.
