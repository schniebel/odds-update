composition folder - crossplane pipeline definitions (groups resources together in a logical pipeline)

functions folder - functions to support composition pipelines

xrd folder - holds custom resource definitions that defines the pipeline for kubernetes

test folder - configuration for all gcp/ aws cloud resources meant for prod env.

prod folder - configuration for all gcp/ aws cloud resources meant for test env.

provider folder - crossplane resources that must be in place so that crossplane knows what the resources in the analytics folder are.

providerConfig folder - crossplane resources that configure necessary permissions to deploy to the cloud providers.


