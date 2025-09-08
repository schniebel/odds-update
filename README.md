# odds-update
For Sr. Infrastructure Engineer take home exercise

## Bootstrapping System
To bootstrap entire system, run `bootstrap.sh`, which will do the following:
- run `main.tf`, which creates the `infra-ops` eks cluster
- bootstrap flux onto the cluster, setting this repo's main branch as the Universal Control Plane source of truth.

requirements:
- aws cli
- flux cli

Once bootstrap is complete, and `flux` is deployed onto `infra-ops`, it will deploy the rest of the stack in a specified order, to avoid race conditions. Order specified by `kind: kustomization` resources in the cluster/infra-ops folder.
