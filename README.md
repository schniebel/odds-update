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

## Odds Update Flow

<img width="3840" height="891" alt="odds-update-flow" src="https://github.com/user-attachments/assets/6225b758-49c6-44a3-9dc2-68f165f9413c" />

### Vendor Feed → Ingestion Adapter
A third-party sports data provider sends a real-time event, either via HTTP push, WebSocket, Kafka, or Pub/Sub.
The ingestion adapter (running in the ingestion cluster) receives the event, validates it, and normalizes it into the platform’s canonical schema.

### Adapter → Core Kafka (raw.events.*)
The adapter produces the normalized message into the Core Kafka cluster under the appropriate raw.events.{league} topic.
These topics serve as the authoritative staging area for all incoming events.

### Core Kafka (raw.events.*) → Odds Compute
Odds compute services (in the compute cluster) consume the raw events.
Each service enriches the event using Postgres (reference/config data) and Redis (cached lookups or hot state).

### Odds Compute → Core Kafka (odds.updates.internal.*)
The odds processor publishes the computed odds update to an internal Core Kafka topic (odds.updates.internal.{league}).
Each message includes metadata headers like league, game_id, player_id, and model_version.

### Internal Topic → Analytics Consumers
Analytics sinks subscribe to odds.updates.internal.* and stream results into ClickHouse (for real-time queries) and S3/Parquet (for long-term analysis by quants).

### Internal Topic → Replication Hop → Edge Kafka
A replication process (MirrorMaker 2 or Confluent Replicator) continuously reads the internal odds topics from Core and writes them to Edge Kafka, renaming them as odds.updates.public.*.
This hop enforces schema compatibility and sanitizes any internal-only fields.

### Edge Kafka → External Consumers
External consumers in multiple accounts or clouds subscribe to the odds.updates.public.* topics on Edge Kafka using private networking.
This allows partners to reliably consume real-time odds updates without touching the private Core.

## Analytics
All odds updates from Core Kafka (odds.updates.internal.*) are written to S3 using a Kafka Connect S3 Sink, creating a Bronze layer of partitioned Parquet files (organized by league and date). 

A scheduled Spark job (running via the Spark Operator in the analytics cluster) processes the Bronze data into a Silver layer. In this step, records are deduplicated, schemas normalized, and small files compacted into query-efficient Parquet.

We then expose the Silver layer to BigQuery BigLake, enabling analysts and quants to query the data directly in S3 using SQL. 

see [analytics](https://github.com/schniebel/odds-update/tree/main/analytics) folder for spark operator and pyspark source code.

## Observability
We run a central [Grafana](https://grafana.com/) in the `infra-ops` cluster. All clusters (including `infra-ops`) run [Prometheus](https://prometheus.io/) for metrics and [Loki](https://grafana.com/oss/loki/) for logs. Managed Kafka (Core MSK and Edge MSK) is monitored via CloudWatch, with vendor specific dashboards as needed. Central Grafana wires it all together as datasources.

## Alerting
There will be a central [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) instance deployed to `infra-ops`, configured to push alerts defined in prometheus/ Cloudwatch to a `#monitoring` slack channel, with varying levels of severity (`warning`, `critical`, `page`).

## CI/CD
As mentioned above, deploying cloud resources is done by defining the resources in yaml, and pointing at those resources via `kind: kustomization`. Flux and Crossplane take care of the provisioning and state management for these resources.

For the application layer, images are built via AWS Cloud Build. Build steps are defined in `cloudspec.yaml` file (example for analytics python image). Each `cloudspec.yaml` has steps for testing, building, tagging, and ultimately pushing to ECR.

Once the tagged image makes to to ECR, flux will pull in the latest images based on [imageUpdateAutomation resources defined in this repo.](https://github.com/schniebel/odds-update/tree/main/infra-ops/image-automation), which are polling the ECR repo, looking for newer tagged images. If a newer image is detected, a commit is automatically made to the relevent `helmrelease` resource that deploys the `sparkApplication`, deploying the new image.

### Tagging strategy
If an images is meant to be deployed to test, it will be tagged with a unix timestamp. if prod, a semver tag is used. Having this different tagging strategy allows us to have test and prod images pushed to the same ECR repo, because flux is configured to only care about one of those tagging strategies depending on if its for test or prod.
