# Deploying Tally to Kubernetes

Minimal manifests for running Tally on a local cluster (kind, k3d, minikube)
or any managed cluster. They demonstrate the deployment shape — replicated
stateless ingest in front, config via env — not a production database setup
(in production you'd point `DATABASE_URL` at a managed Postgres instead).

```bash
# Local cluster example with kind:
kind create cluster --name tally

# Build and load the image
docker build -t tally:local ..
kind load docker-image tally:local --name tally

# Deploy everything
kubectl apply -f postgres.yaml
kubectl apply -f tally.yaml

# Apply migrations (once postgres is Running)
kubectl exec deploy/postgres -- psql -U tally -d tally -f /dev/stdin < ../../migrations/001_init.sql
kubectl exec deploy/postgres -- psql -U tally -d tally -f /dev/stdin < ../../migrations/002_counts_minute.sql

# Talk to it
kubectl port-forward svc/tally 8080:8080
curl -X POST localhost:8080/v1/events -d '{"event_id":"k8s-1","name":"page_view","distinct_id":"u1"}'
```

Notes:

- `tally.yaml` runs 2 replicas of the memory-queue mode. For the durable
  (kafka) mode you'd add a Redpanda/Kafka deployment or point
  `KAFKA_BROKERS` at a managed cluster, then split MODE=ingest and
  MODE=worker into two Deployments that scale independently — the env vars
  already support it.
- Probes hit `/healthz`; resources are sized for a laptop cluster.
