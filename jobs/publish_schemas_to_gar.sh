#!/usr/bin/env bash

# Publish the generated mozilla-pipeline-schemas to Google Artifact Registry
# (GAR), both as a generic tarball and as a FROM-scratch OCI image.
#
# Required environment variables (set by the probe_scraper DAG):
#   MPS_REPO_URL, MPS_BRANCH, GCP_PROJECT, GAR_LOCATION, GAR_REPOSITORY,
#   GENERIC_REPOSITORY, GAR_IMAGE, CRANE_VERSION

set -euo pipefail

# Ensure git/curl are available
if ! command -v git >/dev/null || ! command -v curl >/dev/null; then
  apt-get update
  apt-get install -y --no-install-recommends git curl ca-certificates
fi

# Install crane for daemonless OCI image builds/pushes.
curl -sSL "https://github.com/google/go-containerregistry/releases/download/${CRANE_VERSION}/go-containerregistry_Linux_x86_64.tar.gz" \
  | tar -xz -C /usr/local/bin crane

workdir="$(mktemp -d)"

# Clone the published schemas (public repo, HTTPS, no credentials needed)
git clone --depth 1 --branch "${MPS_BRANCH}" "${MPS_REPO_URL}" "${workdir}/mozilla-pipeline-schemas"
cd "${workdir}/mozilla-pipeline-schemas"
short_ref="$(git rev-parse --short=10 HEAD)"

# Build the schemas tarball (matches the deploy-artifacts.yml git archive)
mkdir -p "${workdir}/image-context"
git archive --format tgz --prefix mozilla-pipeline-schemas/ \
  -o "${workdir}/image-context/schemas.tar.gz" HEAD schemas/

# Upload the tarball to the generic Artifact Registry repository
if ! gcloud artifacts generic upload \
  --location="${GAR_LOCATION}" \
  --project="${GCP_PROJECT}" \
  --repository="${GENERIC_REPOSITORY}" \
  --package="${GAR_IMAGE}" \
  --version="${short_ref}" \
  --source="${workdir}/image-context/schemas.tar.gz" 2> "${workdir}/upload_err.log"; then
  if grep -qi "already exists" "${workdir}/upload_err.log"; then
    echo "Generic artifact version ${short_ref} already exists, skipping upload."
  else
    cat "${workdir}/upload_err.log" >&2
    exit 1
  fi
fi

# Authenticate crane to GAR using the pod's workload-identity service account
crane auth login -u oauth2accesstoken -p "$(gcloud auth print-access-token)" \
  "${GAR_LOCATION}-docker.pkg.dev"

# Build a FROM-scratch image containing /schemas.tar.gz and push it (tag + latest)
cd "${workdir}/image-context"
tar -cf layer.tar schemas.tar.gz
image="${GAR_LOCATION}-docker.pkg.dev/${GCP_PROJECT}/${GAR_REPOSITORY}/${GAR_IMAGE}"
crane append -f layer.tar -t "${image}:${short_ref}"
crane tag "${image}:${short_ref}" latest
