#!/bin/bash
#
# Checks what the release workflow registered in siren for a release: the
# images of the siren automation PR against the ones the trigger-siren job
# takes from the release's packer manifest.
#
# usage: check-cloud-images.sh <X.Y.Z> <out-dir>
#
# Env: REPO (default scylladb/vector-store), SIREN_REPO (default
# scylladb/siren).
# Side effects: read-only gh calls; writes only into <out-dir>.
# Output files in <out-dir>:
#   cloud-images.json   the release's packer manifest (a release asset)
#   siren.diff          the diff of the siren automation PR
#   siren-expected.txt  "<arch> <aws|gcp> <image>" lines that trigger-siren
#                       takes from the manifest: the us-east-1 AMI of the last
#                       amazon-ebs build of each architecture and the last
#                       googlecompute image. A re-run appends its builds to
#                       the manifest, so only the last build of each counts.
#   siren-actual.txt    the same lines, read from the siren PR
# A line per check goes to stdout, starting with "ok:" or "FAIL:".
# Exit status: 0 when every check passes, 1 when one fails, 2 on a usage error.

set -euo pipefail

[[ $# -eq 2 ]] || {
    echo "usage: $0 <X.Y.Z> <out-dir>" >&2
    exit 2
}

version=$1
out=$2
repo=${REPO:-scylladb/vector-store}
siren_repo=${SIREN_REPO:-scylladb/siren}
status=0

ok() { echo "ok: $*"; }
fail() {
    echo "FAIL: $*"
    status=1
}

mkdir -p "${out}"

gh release download "${version}" -R "${repo}" --clobber \
    -p "vector-store-cloud-images-${version}.json" -O "${out}/cloud-images.json" || {
    fail "release ${version} has no vector-store-cloud-images-${version}.json asset"
    exit "${status}"
}

jq -r '
    ([.builds[] | select(.builder_type == "amazon-ebs")] | group_by(.custom_data.arch)[] | last
        | .custom_data.arch + " aws "
            + (.artifact_id | split(",")[] | select(startswith("us-east-1:")) | sub("^us-east-1:"; ""))),
    ([.builds[] | select(.builder_type == "googlecompute")] | last | "amd64 gcp " + .artifact_id)
    ' "${out}/cloud-images.json" | sort > "${out}/siren-expected.txt"

title="feat(vectorstore): add version ${version} [automation]"
pr=$(gh pr list -R "${siren_repo}" --search "add version ${version} in:title" --state all \
    --json number,title -q ".[] | select(.title == \"${title}\") | .number" | head -1)
if [[ -z ${pr} ]]; then
    fail "no ${siren_repo} PR titled '${title}'"
    exit "${status}"
fi
gh pr diff "${pr}" -R "${siren_repo}" > "${out}/siren.diff"
# An added "images:" or "images-arm64:" line opens the images of that
# architecture; each "aws:" or "gcp:" line under it names one image, as
# "<region or project path>/<image>".
awk '
    /^\+ *images:/ { arch = "amd64" }
    /^\+ *images-arm64:/ { arch = "arm64" }
    arch && /^\+ *(aws|gcp): / {
        field = $2; sub(/:$/, "", field)
        image = $3; sub(/.*\//, "", image)
        print arch, field, image
    }' "${out}/siren.diff" | sort > "${out}/siren-actual.txt"

echo "siren PR: $(gh pr view "${pr}" -R "${siren_repo}" --json url,state -q '.url + " (" + .state + ")"')"
if diff "${out}/siren-expected.txt" "${out}/siren-actual.txt" > /dev/null; then
    ok "siren PR #${pr} carries exactly the images trigger-siren took from the manifest:"
    sed 's/^/  /' "${out}/siren-actual.txt"
else
    fail "siren PR #${pr} differs from the manifest (< manifest, > PR):"
    diff "${out}/siren-expected.txt" "${out}/siren-actual.txt" | sed 's/^/  /' || true
fi

exit "${status}"
