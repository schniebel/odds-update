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
