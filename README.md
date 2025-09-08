# odds-update
For Sr. Infrastructure Engineer take home exercise

## Bootstrapping System
To bootstrap entire system, run `bootstrap.sh`, which will do the following:
- run `main.tf`, which creates the `infra-ops` eks cluster
- bootstrap flux onto the cluster, setting this repo's main branch as the Universal Control Plane source of truth.

requirements:
- aws cli
- flux cli

This setup is using a combination of gitops tooling ([flux](https://fluxcd.io/) and [Crossplane](https://www.crossplane.io/)) to create a universal control plane. A single source of truth (this repo) is being watched by the flux and crossplane operators to create cloud resources, configure them, and manage the deployment of workloads onto them (in the case of an eks cluster)

Once bootstrap is complete, and `flux` is deployed onto `infra-ops`, it will create the rest of the stack in a specified order, to avoid race conditions. Order specified by `kind: kustomization` resources in the cluster/infra-ops folder.

Any cloud resources (kafka clusters, eks clusters, VPCs, redis, postgress instances) are deployed via crossplane. The configuration of which is in the crossplane folder. 

## Odds Update Flow -  Kafka Integration Path

<img width="3840" height="796" alt="odds-update-flow _ Mermaid Chart-2025-09-08-201059" src="https://github.com/user-attachments/assets/6cc1117a-3d73-474c-9fd4-506bb1ae73e4" />


### Vendor Kafka → Core Kafka (via PrivateLink + MSK Connect)
The third-party sports data provider hosts their own Kafka cluster in a separate AWS account.
We consume their topics privately through AWS PrivateLink. On our side, an MSK Connect (MirrorSourceConnector) replicator reads from the vendor’s vendor.raw.events.{league} topics and writes into our Core MSK cluster as raw.events.{league}.

Authentication: Replication uses SASL/SCRAM or mTLS, with vendor-provided per-topic ACLs.

Normalization: Lightweight Single Message Transforms (SMTs) enforce canonical keying (game_id) and inject required headers (league, game_id, event_id, model_version).

Schema enforcement: All records flow through AWS Glue Schema Registry, which enforces BACKWARD or FULL compatibility on raw.events.*.

Dead-letter queue (DLQ): Malformed or deserialization failures are routed into a raw.events.dlq topic to prevent backpressure.

These raw.events.* topics in Core Kafka serve as the authoritative staging area for all incoming events.

### Core Kafka (raw.events.*) → Odds Compute
Odds compute services (in the compute cluster) consume from raw.events.*. Each service enriches the events using Postgres (reference/config data) and Redis (cached lookups or hot state), then computes the odds update.

### Odds Compute → Core Kafka (odds.updates.internal.*)
The odds processor publishes computed odds updates into internal Core Kafka topics (odds.updates.internal.{league}).
Each message includes metadata headers such as league, game_id, player_id, and model_version.

### Odds Compute → Core Kafka (odds.updates.internal.*)
The odds processor publishes the computed odds update to an internal Core Kafka topic (odds.updates.internal.{league}).
Each message includes metadata headers like league, game_id, player_id, and model_version.

### Internal Topics → Analytics Consumers
Analytics sinks subscribe to odds.updates.internal.* and stream results into ClickHouse (for real-time queries) and S3/Parquet (for long-term analysis). A scheduled Spark job compacts and normalizes these datasets for analysts and quants.

### Internal Topic → Replication Hop → Edge Kafka
A replication process (MirrorMaker 2) continuously reads from odds.updates.internal.* and writes into Edge Kafka as odds.updates.public.*.

Sanitization: Internal-only fields are stripped.

Schema enforcement: Public schemas are validated against the registry to ensure compatibility.

DLQ: Any records that fail sanitization or schema validation are routed to odds.updates.public.dlq.

### Edge Kafka → External Consumers
External partners subscribe to the odds.updates.public.* topics on Edge Kafka over private networking (PrivateLink or VPC peering). This lets partners consume real-time odds updates reliably without touching Core.

## Analytics
All odds updates from Core Kafka (odds.updates.internal.*) are written to S3 using a Kafka Connect S3 Sink, creating a Bronze layer of partitioned Parquet files (organized by league and date). 

A scheduled Spark job (running via the Spark Operator in the analytics cluster) processes the Bronze data into a Silver layer. In this step, records are deduplicated, schemas normalized, and small files compacted into query-efficient Parquet.

We then expose the Silver layer to BigQuery BigLake, enabling analysts and quants to query the data directly in S3 using SQL. 

<img width="3840" height="216" alt="analytics-flow" src="https://github.com/user-attachments/assets/02b89abf-e66d-49f5-92a4-707ce547aed1" />

see [analytics](https://github.com/schniebel/odds-update/tree/main/analytics) folder for spark operator and pyspark source code.

## Observability
We run a central [Grafana](https://grafana.com/) in the `infra-ops` cluster. All clusters (including `infra-ops`) run [Prometheus](https://prometheus.io/) for metrics and [Loki](https://grafana.com/oss/loki/) for logs. Managed Kafka (Core MSK and Edge MSK) is monitored via CloudWatch, with vendor specific dashboards as needed. Central Grafana wires it all together as datasources.

## Alerting
There will be a central [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) instance deployed to `infra-ops`, configured to push alerts defined in prometheus/ Cloudwatch to a `#monitoring` slack channel, with varying levels of severity (`warning`, `critical`, `page`).

## CI/CD
As mentioned above, deploying cloud resources is done by defining the resources in yaml, and pointing at those resources via `kind: kustomization`. Flux and Crossplane take care of the provisioning and state management for these resources.

For the application layer, images are built via [AWS Cloud Build](https://docs.aws.amazon.com/codebuild/) Build steps are defined in `cloudspec.yaml` file (example for analytics python image). Each `cloudspec.yaml` has steps for testing, building, tagging, and ultimately pushing to ECR.

Once the tagged image makes to to ECR, flux will pull in the latest images based on [imageUpdateAutomation resources defined in this repo.](https://github.com/schniebel/odds-update/tree/main/infra-ops/image-automation), which are polling the ECR repo, looking for newer tagged images. If a newer image is detected, a commit is automatically made to the relevent `helmrelease` resource that deploys the `sparkApplication`, deploying the new image.

### Tagging strategy
If an images is meant to be deployed to test, it will be tagged with a unix timestamp. if prod, a semver tag is used. Having this different tagging strategy allows us to have test and prod images pushed to the same ECR repo, because flux is configured to only care about one of those tagging strategies depending on if its for test or prod.
