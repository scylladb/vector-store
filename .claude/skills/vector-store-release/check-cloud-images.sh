#!/bin/bash
#
# Checks the cloud images the release workflow built for a release and what it
# registered in siren for them:
# - from the log of the run's build-cloud-images job: the base image of each
#   build (the template takes the newest published one, so it is recorded
#   nowhere else for GCP), the install script's kernel and sudo checks, and
#   any provisioning step that failed without failing the build;
# - from the release's packer manifest: the AWS regions each AMI was copied
#   to, against the regions in the release's packer template;
# - the images of the siren automation PR against the ones the trigger-siren
#   job takes from the manifest.
#
# usage: check-cloud-images.sh <X.Y.Z> <release-run-id> <out-dir>
#
# Env: REPO (default scylladb/vector-store), SIREN_REPO (default
# scylladb/siren).
# Side effects: read-only gh calls; writes only into <out-dir>.
# Output files in <out-dir>:
#   build-cloud-images.log  log of the run's build-cloud-images job (its
#                       latest attempt)
#   build-cloud-images.txt  the same without colour codes and job and step
#                       columns
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

[[ $# -eq 3 ]] || {
    echo "usage: $0 <X.Y.Z> <release-run-id> <out-dir>" >&2
    exit 2
}

version=$1
run=$2
out=$3
repo=${REPO:-scylladb/vector-store}
siren_repo=${SIREN_REPO:-scylladb/siren}
status=0

ok() { echo "ok: $*"; }
fail() {
    echo "FAIL: $*"
    status=1
}
# A file of the release's tag, as the workflow checked it out.
at_tag() {
    gh api -H 'Accept: application/vnd.github.raw' "repos/${repo}/contents/$1?ref=${version}"
}

mkdir -p "${out}"

# --- The image build log ---------------------------------------------------

job=$(gh run view "${run}" -R "${repo}" --json jobs \
    -q '.jobs[] | select(.name == "build-cloud-images") | .databaseId')
if [[ -z ${job} ]]; then
    fail "run ${run} has no build-cloud-images job"
else
    gh run view --job "${job}" -R "${repo}" --log > "${out}/build-cloud-images.log"
    # Lines are "<job>\t<step>\t<timestamp> <text>", the text coloured; gh
    # writes the escape of a colour code as an ESC byte or as the two
    # characters "^[".
    sed -E $'s/(\x1b|\\^\\[)\\[[0-9;]*m//g' "${out}/build-cloud-images.log" \
        | cut -f3- | sed -E 's/^[0-9]{4}-[0-9-]+T[0-9:.]+Z //' \
        > "${out}/build-cloud-images.txt"
    log=${out}/build-cloud-images.txt

    # packer names the base of an amazon-ebs build when it finds the AMI and
    # of a googlecompute build when it starts from the image, once per build.
    builds=$(grep -cE '(amazon-ebs: Found Image ID|googlecompute: Using image): ' "${log}" || true)
    echo "base images and checks of the ${builds} builds in the log, in build order:"
    grep -E '(amazon-ebs: Found Image ID|googlecompute: Using image|kernel check|sudo check)' "${log}" \
        | sed -E 's/^(==> | +)//; s/^/  /'

    # Counted against the builds the release's workflow runs, not the ones
    # the log shows, so that a log missing a build cannot pass.
    expected=$(at_tag .github/workflows/release.yaml | grep -cE '^[[:space:]]*packer build ' || true)
    if [[ ${builds} -eq ${expected} && ${expected} -gt 0 ]]; then
        ok "the log shows all ${expected} image builds of the release workflow"
    else
        fail "the log shows ${builds} image builds, the release workflow runs ${expected}"
    fi

    # A template without a check prints nothing for it, so the count is
    # expected only where the release's install script has the check.
    installer=$(at_tag packer/files/vector_store_install_image)
    for check in "kernel:def check_kernel" "sudo:def use_classic_sudo"; do
        name=${check%%:*}
        if ! grep -q "${check#*:}" <<< "${installer}"; then
            ok "${version}'s install script has no ${name} check"
            continue
        fi
        passed=$(grep -c "${name} check passed:" "${log}" || true)
        if [[ ${passed} -eq ${expected} && ${expected} -gt 0 ]]; then
            ok "${name} check passed in all ${expected} builds"
        else
            fail "${name} check passed in ${passed} of ${expected} builds"
        fi
    done

    # The install script's run() prints "Error while running:" for every
    # failed command before raising; some callers catch it and go on (swap,
    # pip, node_exporter, process_exporter), so the build still succeeds.
    if grep -nE 'Error while running:' "${log}" > "${out}/image-errors.txt"; then
        fail "provisioning commands failed without failing the build (line numbers in build-cloud-images.txt):"
        cut -c1-200 "${out}/image-errors.txt" | sed 's/^/  /'
    else
        ok "no provisioning command failed"
    fi
fi

# --- The packer manifest ---------------------------------------------------

gh release download "${version}" -R "${repo}" --clobber \
    -p "vector-store-cloud-images-${version}.json" -O "${out}/cloud-images.json" || {
    fail "release ${version} has no vector-store-cloud-images-${version}.json asset"
    exit "${status}"
}

regions=$(at_tag packer/vector-store-template.json \
    | jq -r '[.variables.aws_region] + (.variables.aws_regions | split(","))
             | map(select(. != "")) | unique | join(",")')
while read -r arch copied; do
    if [[ ${copied} == "${regions}" ]]; then
        ok "${arch} AMI copied to every template region: ${copied}"
    else
        fail "${arch} AMI copied to ${copied}, template regions are ${regions}"
    fi
done < <(jq -r '[.builds[] | select(.builder_type == "amazon-ebs")] | group_by(.custom_data.arch)[] | last
        | .custom_data.arch + " " + ([.artifact_id | split(",")[] | split(":")[0]] | unique | join(","))' \
    "${out}/cloud-images.json")

# --- The siren PR ----------------------------------------------------------

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
