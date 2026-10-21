# REST autoscaler

This autoscaler turns Trino clusters on and off through a small REST API, so any service that can start and stop Trino clusters can back it.
trino-lb keeps no cluster state of its own; the service behind the API is the source of truth.

The API is specified in [`rest-api.yaml`](./rest-api.yaml) (OpenAPI 3.1):

| Request | Response |
| --- | --- |
| `GET /v1/clusters/{cluster}` | `200 {"activated": bool, "ready": bool}`, `404` for an unknown cluster |
| `PATCH /v1/clusters/{cluster}` with `{"activated": bool}` | `200` with the updated status, `404` for an unknown cluster |

`{cluster}` is the trino-lb cluster name.
`activated` is whether the cluster is meant to run, `ready` whether it can take queries.

## Config

```yaml
clusterAutoscaler:
  rest:
    endpoint: http://cluster-scaler.example.svc.cluster.local:8081
    # Optional headers sent with every request, e.g. for authentication
    headers:
      Authorization:
        fromFile:
          path: /var/run/secrets/tokens/cluster-scaler
          prefix: "Bearer "
      X-Api-Version:
        value: "1"
```

The API paths are joined onto `endpoint`, so an endpoint with a path prefix must end in `/` (e.g. `http://scaler:8081/api/`).
Requests time out after 5 seconds.

A header is either a literal `value` or read `fromFile`. A file is re-read on every request, so a rotated credential (e.g. a projected Kubernetes ServiceAccount token) is picked up; its content is trimmed and appended to the optional `prefix`.

If the clusters' coordinators run without an authenticator, `credentials` can be left out of the `trinoClusters` entries.
trino-lb then logs into the Web UI (to poll cluster stats) as user `trino-lb` with an empty password.

A cluster entry's optional `webUiHeaders` (same shape as `headers` above) go with every stats poll.
With `Authorization` among them, trino-lb skips the login and sends that header instead, e.g. a bearer token for a coordinator with `web-ui.authentication.type=jwt`.
Trino checks a token only on a secure request, so a coordinator reached over plain HTTP with `http-server.process-forwarded=true` also needs `X-Forwarded-Proto: https`:

```yaml
    trinoClusters:
      - name: tier-1-0
        endpoint: http://tier-1-0-coordinator.trino.svc.cluster.local:8080
        webUiHeaders:
          Authorization:
            fromFile:
              path: /var/run/secrets/trino-web-ui/token
              prefix: "Bearer "
          X-Forwarded-Proto:
            value: https
```

```yaml
trinoClusterGroups:
  tier-1:
    maxRunningQueries: 10
    autoscaling:
      upscaleQueuedQueriesThreshold: 1
      downscaleRunningQueriesPercentageThreshold: 70
      drainIdleDurationBeforeShutdown: 10m
      minClusters:
        - timeUtc: 00:00:00 - 23:59:59
          weekdays: Mon - Son
          min: 0
    trinoClusters:
      - name: tier-1-0
        endpoint: http://tier-1-0-coordinator.trino.svc.cluster.local:8080
```

## Behaviour

- trino-lb sends `PATCH` with the desired state on every reconcile, so the service must treat it as idempotent.
- A failed `PATCH` (transport error, timeout or non-2xx response) is returned as an error and retried on the next reconcile.
- A failed `GET` (transport error, timeout, non-2xx response, or a `200` whose body is not a valid status) fails that reconcile and leaves every cluster's state as it was, so a draining cluster is never deactivated because the API was briefly unreachable.

## Kubernetes example

To authenticate with a projected ServiceAccount token, mount one for the audience the service expects and send it as shown in the config above:

```yaml
volumes:
  - name: scaler-token
    projected:
      sources:
        - serviceAccountToken:
            audience: cluster-scaler
            path: cluster-scaler
volumeMounts:
  - name: scaler-token
    mountPath: /var/run/secrets/tokens
    readOnly: true
```
