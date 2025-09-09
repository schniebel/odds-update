### GitOps control plane

The infra-ops EKS cluster runs Flux and Crossplane and acts as the control plane for all environments. Flux watches this repo and reconciles resources in a strict order using `Kustomization.dependsOn`.

Order of operations: Namespaces → External Secrets Operator → Secrets → Crossplane install → Crossplane providers/compositions/managed resources → Observability. Once Crossplane provisions cloud infra (including EKS clusters), Flux uses per-cluster kubeConfig secrets to push workloads (e.g., odds-compute and analytics) into those target clusters.

Separation of concerns:

infra-ops: platform primitives (secrets, operators, Crossplane, observability) and all cloud provisioning via Crossplane.

workload clusters (odds-compute, analytics): application charts deployed by Flux from this repo, driven by the control plane via kubeConfig secrets. Most of the workloads are deployed via [kind: helmRelease](https://fluxcd.io/flux/components/helm/helmreleases/) resources. which is how flux deploys helm charts.

Result: A single source of truth (this repo) declaratively manages infra and apps across multiple clusters with reproducible ordering, automated drift correction, and clear ownership boundaries.

<img width="3840" height="3069" alt="flux-crossplane-flow" src="https://github.com/user-attachments/assets/1e46702e-a8c2-4457-8c23-3ef1c0478ff8" />
