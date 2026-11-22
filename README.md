# demo-spark-polaris-iceberg-k8s
##### Spark jobs running **inside** Kubernetes (minikube), writing Iceberg tables through Apache Polaris, with MinIO storage and Postgres as Polaris's metastore, in a production-style layout

Spark runs as `SparkApplication`s of the Apache Spark Kubernetes Operator: one
driver pod and executor pods per run. The jobs never see a storage key. They
authenticate to Polaris with their own principal, and Polaris **vends**
short-lived MinIO credentials through MinIO's STS `AssumeRole`.

| Component | What it is here |
|---|---|
| `postgres` | plain manifest, PostgreSQL 17.2 with a PVC: Polaris metastore (`relational-jdbc`) |
| `minio` | official MinIO chart 5.4.0, bucket `warehouse`, user `lake-user` scoped to it (STS source) |
| `polaris` | official Apache Polaris chart **1.9.0**, catalog `lakehouse`, credential vending via `storageName: lake` |
| `spark-operator` | Apache Spark Kubernetes Operator **1.1.0** (Helm chart 1.9.0), CRD `spark.apache.org/v1` |
| `spark-jobs` image | `apache/spark:4.1.4` + Iceberg 1.12.0 JARs + the job code in `jobs/src` |
| `trino` | optional: official Trino chart 1.45.0 (Trino 484), to read the same tables from a second engine |

## Prerequisites

| Tool | Why |
|---|---|
| `docker` | minikube driver, and pulling images on the host |
| `minikube` **1.29+** | `--static-ip` needs 1.29 or newer |
| `kubectl`, `helm` | deploy everything |
| `jq` | read Polaris API responses (steps 6, 10, 11) |
| `envsubst` (package `gettext`), `python3` | used by `scripts/submit.sh` |
| `curl` | download the Iceberg JARs, call Polaris |
| Java 17 | only for the unit tests on the laptop (step 12) |

The Polaris + MinIO part is adapted from
[demo-iceberg-polaris-multi-catalog-minio](https://github.com/rmmbdev/demo-iceberg-polaris-multi-catalog-minio),
which runs two catalogs on two MinIO instances and queries them from Trino.
This repo uses one catalog and focuses on running Spark in the cluster.

Run every command in order, from the repository root. Nothing here has been run
end to end yet: the Helm values were rendered with `helm template` and the
`SparkApplication`s validated against the operator's real CRD, but expect to
fix small things on the first run.

---

## How production ships and runs Spark jobs (read first)

**Spark jobs are not shipped like Airflow DAGs.** git-sync fits Airflow because
the scheduler is a long-running process that *keeps re-reading* DAG files. A
Spark job is a **one-shot** set of pods (driver + executors) created per run
and then gone. There's nothing long-running to keep in sync, and "whatever
`main` is right now" would make runs unreproducible.

What teams do instead:

1. **Code baked into an image (the standard, and what this repo does).** The
   image holds Spark, the connector JARs and the job code. Each change gets a
   new tag (in production, CI tags it with the git commit). A run refers to an
   exact tag, so you always know what ran, and rolling back means using the
   previous tag.
2. **Code in object storage.** A generic Spark image fetches `job.py` from
   `s3a://...` at start (common on EMR/Dataproc-style platforms). Cheaper to
   iterate on, but code and image can drift apart.
3. **git-sync as an init container.** Possible, but rare: you'd pin a commit
   anyway, and then option 1 is simpler.

**Submitting is easy:** with an operator, a run is a Kubernetes resource.
`kubectl create -f run.yaml` and the operator creates the driver, which creates
the executors. In production an **orchestrator** does this on a schedule: an
Airflow task (the DAG itself *is* git-synced) renders the `SparkApplication`
with a unique run name and the current image tag, creates it, waits for its
state, and fails the task if the job failed. `scripts/submit.sh` does exactly
that, so you can see the moving parts without Airflow.

```
git ──► docker build ──► image spark-jobs:<tag> ─┐
                                                 ▼
 Airflow / scripts/submit.sh ──► SparkApplication (template + run name + tag)
                                                 │
                         spark-operator ─────────┘
                                │ creates
                                ▼
                         driver pod ──► executor pods
                                │
             Polaris (REST, OAuth) ──► vended STS creds ──► MinIO
```

---

## Layout

```
k8s/
  secrets/      *.env                      → Kubernetes Secrets
  manifests/    postgres.yaml
  values/       minio.yaml  polaris.yaml  spark-operator.yaml  trino.yaml
  catalogs/     lakehouse.json             → Polaris catalog definition
  spark-apps/   generate-quotes.yaml  quotes-daily.yaml  maintenance.yaml
                                           → SparkApplication templates
jobs/
  Dockerfile    the one image for all jobs
  src/lakehouse/
    spark.py         SparkSession (catalog config comes from the SparkApplication)
    transforms.py    pure DataFrame → DataFrame functions (unit-tested)
    jobs/            entrypoints: generate_quotes, quotes_daily, maintenance
  tests/        pytest, no cluster needed
scripts/
  submit.sh     render a template, create the run, wait for the result
```

Configuration is split the production way:

| Where | What |
|---|---|
| Image (`jobs/`) | Code and JARs, the same in every environment |
| `SparkApplication` (`k8s/spark-apps/`) | Environment: Polaris URI, warehouse, S3 endpoint, sizing, image tag |
| Kubernetes Secret | The Polaris credential, injected as `POLARIS_CREDENTIAL` |

## Addresses

The minikube node has a **static IP**, and services use fixed NodePorts. Polaris
hands clients the catalog's `endpoint` (`http://192.168.49.2:30900`) together
with the vended credentials, and that address works from your laptop **and**
from pods. Polaris itself uses `endpointInternal` (`http://minio:9000`).

| Service | In the cluster | From the laptop |
|---|---|---|
| Polaris REST + management | `http://polaris:8181` | `http://192.168.49.2:30181` |
| MinIO S3 API | `http://minio:9000` | `http://192.168.49.2:30900` |
| MinIO console | | `http://192.168.49.2:30901` (`minioadmin` / `minioadmin`) |
| Trino (optional) | `http://trino:8080` | `http://192.168.49.2:30808` |
| Spark UI of a running driver | | `kubectl port-forward` (step 10) |

## Memory (8 GB minikube)

| Component | Memory |
|---|---|
| Postgres + MinIO + Polaris | ~2.5 GB |
| spark-operator | 1 GB |
| One job run: driver + 1 executor (1g heap + overhead each) | ~2.8 GB |
| Trino (optional) | ~3 GB, so scale it to 0 before running jobs |

---

## 1. Helm repositories

```bash
  helm repo add polaris https://downloads.apache.org/polaris/helm-chart
```

```bash
  helm repo add minio https://charts.min.io/
```

```bash
  helm repo add spark https://apache.github.io/spark-kubernetes-operator
```

```bash
  helm repo add trino https://trinodb.github.io/charts
```

```bash
  helm repo update
```

Check the operator's chart version. **Chart 1.9.0 = operator 1.1.0.** Helm's
`--version` takes the *chart* version, so `--version 1.1.0` would install the
old operator 0.3.0.

```bash
  helm search repo spark/spark-kubernetes-operator --versions | head -3
```

## 2. Cluster and images

```bash
  docker pull postgres:17.2-bookworm && \
  docker pull cgr.dev/chainguard/minio@sha256:e9cf6b99c226e9539ed0f14b6f00e37b07da153c7ba1f4f8709c807574b471ec && \
  docker tag cgr.dev/chainguard/minio@sha256:e9cf6b99c226e9539ed0f14b6f00e37b07da153c7ba1f4f8709c807574b471ec \
    cgr.dev/chainguard/minio:RELEASE.2026-11-16T00-11-40Z && \
  docker pull apache/polaris:1.9.0 && \
  docker pull apache/polaris-admin-tool:1.9.0 && \
  docker pull apache/spark-kubernetes-operator:1.1.0 && \
  docker pull apache/spark:4.1.4-scala2.13-java17-python3-ubuntu
```

`--static-ip`, `--memory` and `--disk-size` only apply when the cluster is
**created**. If a `minikube` profile already exists, delete it first
(`minikube delete`) or use another profile (`-p <name>` on every minikube
command).

```bash
  minikube start --memory=8192 --cpus=6
```

```bash
  minikube image load postgres:17.2-bookworm && \
  minikube image load cgr.dev/chainguard/minio:RELEASE.2026-11-16T00-11-40Z && \
  minikube image load apache/polaris:1.9.0 && \
  minikube image load apache/polaris-admin-tool:1.9.0 && \
  minikube image load apache/spark-kubernetes-operator:1.1.0 && \
  minikube image load apache/spark:4.1.4-scala2.13-java17-python3-ubuntu
```

## 3. Secrets

```bash
  kubectl create secret generic postgres-secrets   --from-env-file=k8s/secrets/postgres.env && \
  kubectl create secret generic polaris-db-secrets --from-env-file=k8s/secrets/polaris-db.env && \
  kubectl create secret generic minio-root         --from-env-file=k8s/secrets/minio-root.env && \
  kubectl create secret generic lake-user-secrets  --from-env-file=k8s/secrets/lake-user.env
```

## 4. Postgres and MinIO

```bash
  kubectl apply -f k8s/manifests/postgres.yaml
```

```bash
  helm upgrade --install minio minio/minio --version 5.4.0 \
    --values k8s/values/minio.yaml --timeout 10m
```

```bash
  kubectl wait --for=condition=available deploy/postgres deploy/minio --timeout=300s
```

The MinIO chart's post-install job creates bucket `warehouse`, policy
`warehouse-rw` and user `lake-user`. Check in the console
(<http://192.168.49.2:30901>): **Identity → Users → lake-user** has `warehouse-rw`.

## 5. Polaris

Polaris 1.9.0 creates its tables in the Postgres schema `polaris_schema` (the
JDBC driver selects it on every connection via `currentSchema=POLARIS_SCHEMA`),
but it **no longer creates the schema itself**: that's left to the DBA. Without
it, bootstrap fails with `ERROR: no schema has been selected to create in`, no
`root` principal exists, and the token request in step 6 returns `null`.
Create the schema first:

```bash
  kubectl exec deploy/postgres -- psql -U postgres -d polaris \
    -c 'CREATE SCHEMA IF NOT EXISTS polaris_schema'
```

Bootstrap creates Polaris's tables in that schema and the `root` principal of
realm `POLARIS`. Polaris 1.9.0 needs a `--credential` for every `--realm`. Check
that the output ends with `Bootstrap completed successfully.`:

```bash
  kubectl run polaris-bootstrap --rm -it --restart=Never \
  --image=apache/polaris-admin-tool:1.9.0 \
  --image-pull-policy=IfNotPresent \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=postgres" \
  --env="quarkus.datasource.password=postgres" \
  --env="quarkus.datasource.jdbc.url=jdbc:postgresql://postgres:5432/polaris" \
  -- bootstrap -r POLARIS -c POLARIS,root,root
```

```bash
  helm upgrade --install polaris polaris/polaris --version 1.9.0 \
    --values k8s/values/polaris.yaml --timeout 10m
```

```bash
  kubectl rollout status deploy/polaris --timeout=600s
```

`401` means Polaris is up and wants a token:

```bash
  curl -s -o /dev/null -w "%{http_code}\n" http://192.168.49.2:30181/api/catalog/v1/config
```

## 6. Catalog and principals

```bash
  export POLARIS=http://192.168.49.2:30181
```

```bash
  export TOKEN=$(curl -s -X POST $POLARIS/api/catalog/v1/oauth/tokens \
    -d "grant_type=client_credentials&client_id=root&client_secret=root&scope=PRINCIPAL_ROLE:ALL" \
    | jq -r '.access_token')
```

The catalog:

```bash
  curl -X POST $POLARIS/api/management/v1/catalogs \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d @k8s/catalogs/lakehouse.json
```

Creating a catalog also creates its `catalog_admin` role, which can only manage
access at first. Let it manage content:

```bash
  curl -X PUT $POLARIS/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}'
```

A principal for the jobs. Its secret is returned **only once**, so it goes
straight into a file (git-ignored):

```bash
  curl -s -X POST $POLARIS/api/management/v1/principals \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principal": {"name": "spark-job", "type": "SERVICE"}}' \
    | jq -r '.credentials.clientId + ":" + .credentials.clientSecret' \
    > k8s/secrets/spark-job.credential
```

The chain **principal → principal role → catalog role → privilege**:

```bash
  curl -X POST $POLARIS/api/management/v1/principal-roles \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principalRole": {"name": "data_engineer"}}'
```

```bash
  curl -X PUT $POLARIS/api/management/v1/principal-roles/data_engineer/catalog-roles/lakehouse \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}'
```

```bash
  curl -X PUT $POLARIS/api/management/v1/principals/spark-job/principal-roles \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principalRole": {"name": "data_engineer"}}'
```

The credential as a Secret. `--from-literal` with `$(...)` drops the file's
trailing newline (`--from-file` would keep it, and Polaris would answer `401`):

```bash
  kubectl create secret generic spark-job-polaris \
    --from-literal=credential="$(cat k8s/secrets/spark-job.credential)"
```

## 7. Spark operator

```bash
  helm upgrade --install spark-operator spark/spark-kubernetes-operator \
    --version 1.9.0 --values k8s/values/spark-operator.yaml
```

```bash
  kubectl rollout status deploy/spark-kubernetes-operator --timeout=300s
```

The chart installs two CRDs and two service accounts: `spark-operator` (the
operator) and `spark` (used by **drivers**, because the driver itself creates
the executor pods). This must print `yes`:

```bash
  kubectl auth can-i create pods --as=system:serviceaccount:default:spark
```

## 8. Build the job image

Download the Iceberg JARs that get baked into the image (git-ignored):

```bash
  mkdir -p jobs/jars && \
  curl -fL -o jobs/jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar && \
  curl -fL -o jobs/jars/iceberg-aws-bundle-1.12.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar
```

Build **inside** minikube, so the cluster has the image without a registry:

```bash
  minikube image build -t spark-jobs:0.1 jobs
```

```bash
  minikube image ls | grep spark-jobs
```

**Bump the tag for every change** (`0.2`, `0.3`, ...) and pass it to
`submit.sh` with `TAG=0.2`. The templates use `imagePullPolicy: IfNotPresent`,
so a reused tag can silently run the old image. In production, CI tags with
the git commit for the same reason.

Why bake the JARs instead of `spark.jars.packages`? With packages, every driver
and executor downloads them from Maven at start: slow, it breaks when the
network does, and it can resolve differently tomorrow.

## 9. Run the pipeline

Each command creates a uniquely named run (`<job>-<UTC timestamp>`), prints its
state transitions and exits non-zero if the job failed, as an Airflow task
would.

**Bronze:** generate 2M synthetic quotes over 7 days. Pods can't see files on
your laptop, so the source data is created in the cluster:

```bash
  scripts/submit.sh generate-quotes --rows 2000000 --start-day 2026-08-01 --days 7
```

**Silver:** load one day:

```bash
  scripts/submit.sh quotes-daily --day 2026-08-02
```

Typical output (illustrative: the state names come from the operator's CRD,
timings and the exact sequence will differ):

```
submitted quotes-daily-20261120-101500 with image spark-jobs:0.1
10:15:01  Submitted
10:15:03  DriverRequested
10:15:10  DriverStarted
10:15:25  DriverReady
10:15:31  RunningHealthy
10:16:10  Succeeded
quotes-daily-20261120-101500: SUCCEEDED
```

The job's own output (the driver pod is `<run>-0-driver`; if yours differs,
take it from `kubectl get pods -l spark-role=driver`):

```bash
  kubectl logs $(kubectl get pods -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1) | grep -A6 loaded
```

Expected: `loaded 2857xx rows for 2026-08-02 (0 invalid skipped)` and a commit
with operation `overwrite`.

## 10. Things to try

**Idempotent rerun.** Run the same day again. The new commit replaces the
partition: `added` = `deleted`, `total` unchanged.

```bash
  scripts/submit.sh quotes-daily --day 2026-08-02
```

**Retries.** A day with no data fails the job's empty-day guard. The template
has `restartPolicy: OnFailure` with `maxRestartAttempts: 2`, so you'll see
`Failed`, then `ScheduledToRestart`, then a new attempt (`<run>-1-driver`), and
finally `FAILED`. Retries help with flaky infrastructure, not with bad input:
that's why production jobs also need guards that fail loudly.

```bash
  scripts/submit.sh quotes-daily --day 2026-12-01
```

**Maintenance.** Load a few more days, then compact and expire snapshots, the
nightly routine of a lakehouse team:

```bash
  for d in 2026-08-03 2026-08-04 2026-08-05; do scripts/submit.sh quotes-daily --day $d; done
```

```bash
  scripts/submit.sh maintenance --table silver.quotes_daily --retain-last 3
```

**The Spark UI of a job in the cluster.** The UI lives in the driver pod and
dies with it. Keep the driver alive for 5 minutes with `--linger`, without
waiting in the terminal:

```bash
  WAIT=0 scripts/submit.sh quotes-daily --day 2026-08-06 --linger 300
```

Wait until the run shows `RunningHealthy` (Ctrl+C to stop watching), so the
driver pod exists:

```bash
  kubectl get sparkapp -w
```

```bash
  kubectl port-forward $(kubectl get pods -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1) 4040:4040
```

Open <http://localhost:4040>. In **Environment**,
`spark.sql.catalog.lake.credential` shows `*********(redacted)` (the
`spark.redaction.regex` in the template). Production clusters run a Spark
History Server instead, which reads event logs from object storage after the
pods are gone.

**Where the commit pointer lives.** Load the table over Polaris's REST API as
`spark-job`, the way Spark does (in a new terminal, run
`export POLARIS=http://192.168.49.2:30181` first):

```bash
  ID=$(cut -d: -f1 k8s/secrets/spark-job.credential); SECRET=$(cut -d: -f2 k8s/secrets/spark-job.credential); \
  STOKEN=$(curl -s -X POST $POLARIS/api/catalog/v1/oauth/tokens \
    -d "grant_type=client_credentials&client_id=$ID&client_secret=$SECRET&scope=PRINCIPAL_ROLE:ALL" | jq -r .access_token); \
  curl -s -H "Authorization: Bearer $STOKEN" \
    $POLARIS/api/catalog/v1/lakehouse/namespaces/silver/tables/quotes_daily | jq -r '."metadata-location"'
```

**Run history.** Runs stay as resources (with their pods, thanks to
`resourceRetainPolicy: Always`) until `ttlAfterStopMillis` (1 day) removes them:

```bash
  kubectl get sparkapp -l app.kubernetes.io/part-of=lakehouse-jobs
```

```bash
  kubectl describe sparkapp <run-name>
```

## 11. Optional: the same tables from Trino

Scale the operator down first if memory is tight
(`kubectl scale deploy/spark-kubernetes-operator --replicas=0`).

```bash
  docker pull trinodb/trino:484 && minikube image load trinodb/trino:484
```

A principal for Trino, with the same role. `TOKEN` from step 6 expires after an
hour, and is gone in a new terminal: rerun the two `export` commands of step 6
first.

```bash
  curl -s -X POST $POLARIS/api/management/v1/principals \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principal": {"name": "trino-svc", "type": "SERVICE"}}' \
    | jq -r '.credentials.clientId + ":" + .credentials.clientSecret' \
    > k8s/secrets/trino-svc.credential
```

```bash
  curl -X PUT $POLARIS/api/management/v1/principals/trino-svc/principal-roles \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"principalRole": {"name": "data_engineer"}}'
```

```bash
  kubectl create secret generic trino-secrets \
    --from-literal=POLARIS_AUTH="$(cat k8s/secrets/trino-svc.credential)" \
    --from-literal=S3_ACCESS_KEY=lake-user \
    --from-literal=S3_SECRET_KEY=lake-secret-0123456789
```

```bash
  helm upgrade --install trino trino/trino --version 1.45.0 \
    --values k8s/values/trino.yaml --timeout 10m
```

```bash
  kubectl rollout status deploy/trino-coordinator --timeout=600s
```

```bash
  kubectl exec -it deploy/trino-coordinator -- trino --server localhost:8080 --user admin \
    --execute 'SELECT CAST(created_at AS date) AS day, count(*) AS quotes FROM lake.silver.quotes_daily GROUP BY 1 ORDER BY 1'
```

Same rows Spark wrote: two engines, one catalog, no copy.

## 12. Unit tests (on the laptop, no cluster)

Needs Java 17 and Python 3.10+. Spark 4.1 doesn't run on newer JDKs such as
Java 25; install 17 next to it (Manjaro/Arch: `sudo pacman -S jdk17-openjdk`)
without changing the system default. The commands below run inside `jobs/`.

```bash
  cd jobs && python -m venv .venv && . .venv/bin/activate && pip install ".[dev]"
```

```bash
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk pytest -q
```

```bash
  ruff check src tests
```

The transforms in `jobs/src/lakehouse/transforms.py` are pure
`DataFrame → DataFrame` functions, so they're tested on a few in-memory rows.
The jobs only wire them to reads and writes.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `minikube start` ignores `--static-ip` | The profile already existed; flags apply only at creation |
| Pods in `ImagePullBackOff` | Image not loaded: `minikube image load <image>` (for `spark-jobs`, rebuild with step 8) |
| Polaris `CrashLoopBackOff` mentioning the schema | Bootstrap (step 5) not run, or run against another database |
| `POST /catalogs` → `400` about the storage type | `SUPPORTED_CATALOG_STORAGE_TYPES` not applied: `kubectl get cm polaris -o yaml` |
| Bootstrap: `no schema has been selected to create in` | Schema `polaris_schema` missing: create it (step 5) and rerun bootstrap |
| Token is `null` right after setup | Bootstrap failed, so there's no `root` principal: rerun step 5 and check for `Bootstrap completed successfully.` |
| Token is `null` / `403` from management calls | Wrong root credential, or `TOKEN` older than an hour |
| Run stays `Submitted` / ends `DriverStartTimedOut` | Not enough memory to schedule the driver: `kubectl describe pod <driver>`; scale Trino down |
| `pods is forbidden` in the driver log | Driver not running as the `spark` service account |
| `KeyError` / `401` / `NotAuthorizedException` | Secret `spark-job-polaris` missing, wrong key, or created with `--from-file` |
| `ForbiddenException` from Polaris | `spark-job` lacks the `data_engineer` role chain (step 6) |
| `403` / `AccessDenied` from S3 | Vending problem: `kubectl logs deploy/polaris \| grep -i -E "sts\|assume"`; check `lake-user` has `warehouse-rw` |
| `kubectl create` says the run already exists | Two submits in the same second; run again |
| `ModuleNotFoundError: lakehouse` in the driver log | Image built from an old Dockerfile; rebuild with a new tag (step 8) |
| `submit.sh`: `envsubst: command not found` | Install `gettext` |
| Code change has no effect | Image tag reused with `IfNotPresent`: build a new tag and pass `TAG=` |

## Teardown

This deletes the **whole** minikube cluster, including anything else in the
same profile. To stop without losing anything, use `minikube stop` instead.

```bash
  minikube stop && minikube delete
```
