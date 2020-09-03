# terraclimate-feedstock
A pangeo-smithy repository for the terraclimate dataset.

## Notes

1. Who does what?

Ideally we cleanly separate the two concerns

1. recipe writer: details about how to e/t/l the data.
2. pangeo-forge: details about how to execute the flow.
  * environment.
  * storage.
  * CLI to register, execute flows.

2. Execution environment?

Reproducibility argues for a Docker image. You could in theory run this
anywhere.

# -----------
# Merge notes from other

1. Add a ServiceAccount

This needs to have read / write access to pangeo-scratch (like `pangeo`) *and* have the ability to start / stop pods for dask-kubernetes. We don't want to give the `pangeo` SA permissions to read / write, so we'll make a new one.

```console
$ kubectl apply -f daskkubernetes.yaml
serviceaccount/pangeo-forge created
role.rbac.authorization.k8s.io/pangeo-forge created
rolebinding.rbac.authorization.k8s.io/pangeo-forge created
```

2. Create a GSA

```
$ gcloud iam service-accounts create pangeo-forge --display-name=pangeo-forge --description="GSA for pangeo-forge. Grant read / write access to gcs://pangeo-scratch."
Created service account [pangeo-forge].
```

2. 

```console
$ gcloud iam service-accounts add-iam-policy-binding \
  --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:pangeo-181919.svc.id.goog[staging/pangeo-forge]" \
      pangeo-forge@pangeo-181919.iam.gserviceaccount.com
Updated IAM policy for serviceAccount [pangeo-forge@pangeo-181919.iam.gserviceaccount.com].
bindings:
- members:
  - serviceAccount:pangeo-181919.svc.id.goog[staging/pangeo-forge]
  role: roles/iam.workloadIdentityUser
etag: BwWuWOjv5tg=
version: 1
```
