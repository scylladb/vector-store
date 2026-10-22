# Releasing binaries from vector-store repositiory

## Releasing vector-store

Building an architecture other than the host's requires qemu plus docker
support for emulated builds. See
https://docs.docker.com/build/building/multi-platform/. You can use
`scripts/prepare-docker-qemu` script to register qemu handlers in docker
engine. Building only the host's native architecture doesn't need qemu.

Release version is based on git annotated tags. You can create a new tag using:

```bash
git tag -a X.Y.Z -m "Release version X.Y.Z"
git push origin X.Y.Z
```

GitHub doesn't support creating annotated tags, so you must do them from local
environment. You can add additional metadata to the tag, for example
`X.Y.Z-dev` for development releases.

In GitHub Actions there is a release workflow that builds the release artifacts
and uploads them to GitHub and DockerHub. It is triggered when a GitHub Release
is published, or manually via Run workflow with `VECTOR_VERSION`, and runs on
both amd64 and arm64 runners. Before building artifacts it runs the full
validator test suite.

The `upload-release-assets` job attaches to the GitHub release the same set of
files `scripts/upload-release` uploads by hand:

- `vector-store-{version}-{amd64,arm64}.tar.gz`
- `vector-store-{version}.cdx.json`
- `vector-store-docker-{version}-{amd64,arm64}.cdx.json`

It runs before `build-cloud-images` on purpose: the packer provisioner installs
the service by downloading the tarball from the release page (see
`packer/files/vector_store_install_image`), so as long as the release carries no
assets the cloud image build cannot succeed.

Only a run started by publishing a release uploads anything. A
`workflow_dispatch` run takes an arbitrary `VECTOR_VERSION` and must not replace
the assets of an already-published release, so it attaches nothing and builds
its images from whatever the release page already carries; if the tarball is
missing there, the image build fails at the download, naming the URL.
Re-running a failed release run keeps its `release` event, so that recovery
path uploads normally.

`push-docker` also tags the image as `scylladb/vector-store:latest`, but only
when `VECTOR_VERSION` is the newest GA release, i.e. the highest `X.Y.Z`
annotated tag in the repository. A patch of an older series or a pre-release
therefore never moves `latest`. To move it anywhere else, retag by hand:

```bash
docker buildx imagetools create --tag scylladb/vector-store:latest scylladb/vector-store:X.Y.Z
```

### Slack announcement

The last job, `announce`, posts the release to `#release-announce` with
`@here`. On its own it only says that the version is released and links the
release notes. For a release users should hear more about, write the
announcement into the release notes as a hidden block, which GitHub does not
render on the release page:

```markdown
<!-- slack-announcement
:rocket: ScyllaDB Vector Store 1.13.0 is out! One fix worth upgrading for, and one trade-off to check first:
:white_check_mark: Searches no longer stall during deletes. With usearch 2.22 each delete on a 136M-row index walked a long run of slots, and at 2 deletes/s p99 search latency reached 880 ms; with usearch 2.26 it stays under 5 ms at up to 50 deletes/s (#634)
:warning: Search throughput is lower in some configurations: −4% on x86 F32, −6% on arm64 F16, −13% on x86 F16, and 1M-vector index builds take 5–20% longer. arm64 F32 is at parity and F16 recall is up 1.3 points. For a half-precision index, prefer BF16 (#634, #645)
:lock: rustls 0.23.46 resolves RUSTSEC-2026-0346 and RUSTSEC-2026-0347 (#647)
Recommended if your workload deletes rows; benchmark first if you serve F16 indexes on x86.
-->
```

The block must open with a line holding exactly `<!-- slack-announcement` and
close with a line holding exactly `-->`; a block that is never closed fails the
job rather than posting the wrong text. Its contents are posted as they are,
so write them in Slack's formatting rather than GitHub's: `*bold*`,
`<https://…|link text>`, `:emoji:`. Slack reads `<…>` as a link or a mention,
so write a literal `<`, `>` or `&` (as in `p99 < 5 ms`) as `&lt;`, `&gt;` or
`&amp;`. The job puts `@here` in front, turns PR
numbers such as `(#634, #645)` into links, and adds a line linking the release
notes.

Aim the announcement at someone deciding whether to upgrade: one line on what
kind of release it is, a `:warning:` line for each change in behaviour, defaults
or performance they should check first, the fixes that matter grouped into a
line or two, and a recommendation on who should upgrade. Leave the rest to the
release notes.

The job reads the notes when it runs, not from the release event, so to correct
a block before the announcement goes out, edit the release notes. If the job
has failed, re-run it after editing. The job summary shows the message it
posted. Slack answers a refused message with HTTP 200, so the job checks the
`ok` field of the reply and fails, naming Slack's error, when it is false.

## Manual release

In case you want to build the release manually, you can use the scripts in
`scripts/` directory. The scripts are designed to be run from the root of the
repository.

Script `scripts/run-with-release-toolchain` can create a correct version for
the `Cargo.toml` file based on annotated tags using `git describe --dirty`.

To build the release you need to git clone repository and switch to the desired
version tag. Then from the root of the repository run:

```bash
./scripts/build-release
```

This will create a tar.gz release in the `target/{amd64,arm64}/release`
directories and per-architecture docker images. The script optionally accepts a subset of
architectures to build as arguments (e.g. `./scripts/build-release arm64`).
Building an architecture matching the host doesn't need qemu — this is how
the release workflow builds each architecture natively on its own per-arch
runner. Building a non-native architecture still requires qemu.

No multi-platform docker image is built locally: the default docker driver
cannot store one in the classic image store. Instead the multi-arch tag is
assembled registry-side from the pushed per-arch images when uploading:

```bash
./scripts/upload-dockers
```

This pushes the per-arch images, combines them into the multi-arch tag with
`docker buildx imagetools create`, and moves `latest` to it under the same
rule as the workflow. The multi-arch tag lists exactly the two architectures:

```
Manifests:
  linux/amd64
  linux/arm64
```

Two details are deliberate here. The per-arch images are combined *by the
digest of their `linux/<arch>` manifest*, not by their tags, because buildx
attaches a provenance attestation to each build: that makes even a
single-platform image an index bundling the image with an extra
`unknown/unknown` manifest, and combining the tags directly would copy those
into the multi-arch tag as well. The attestations remain available on the
per-arch tags (`docker buildx imagetools inspect --format '{{json
.Provenance}}' scylladb/vector-store:{version}-amd64`), they are just not
carried into the combined tag.

The combining also uses `imagetools` rather than `docker manifest create`,
because the latter refuses a source image that is itself an index and fails
with `<image> is a manifest list`.

The build script also generates SBOM files in CycloneDX JSON format:
- `vector-store-{version}.cdx.json` — Cargo dependencies SBOM generated by
  `cargo-cyclonedx`
- `vector-store-docker-{version}-{arch}.cdx.json` — per-architecture Docker
  image SBOM generated by `syft`, covering OS-level packages from the base
  image (one per architecture: amd64, arm64)

After building the release you ought to update the release notes in GitHub and
upload the tar.gz files and both SBOM files to the release. You can use the
upload script to do this:

```bash
./scripts/upload-release
```

This will upload all tar.gz archives and SBOM files to the GitHub release
matching the current version tag. It requires the `gh` CLI to be installed and
authenticated.

## Registering the release in siren

The release workflow registers a GA release with siren automatically: after
`build-cloud-images` uploads the packer manifest, the `trigger-siren` job
extracts the `us-east-1` amd64/arm64 AMI IDs and the GCP image name from it
and hands them to siren's
[add-vectorstore-ver](https://github.com/scylladb/siren/actions/workflows/add-vectorstore-ver.yml)
workflow as a `workflow_dispatch` against siren's default branch. That
workflow opens a `feat(vectorstore): add version X.Y.Z [automation]` PR
against siren's `info/version/versions.yaml`. No manual step is needed for a
GA release.

The dispatch is authenticated with the `SIREN_REPO_TOKEN` Actions secret of
`scylladb/vector-store` (provisioned in VECTOR-854). It must be a *secret*,
not a repository variable of the same name — `secrets.` cannot read
variables — and it must hold a fine-grained PAT owned by `scylladb`, listing
`scylladb/siren` among its repositories, approved by the organization, and
granting the `Actions: read and write` repository permission. `Actions` is
the permission GitHub checks for starting a workflow; `Contents` is not
needed. A PAT also expires, so note its expiry and rotate it before a release
runs into it.

Two caveats:

- Only GA `X.Y.Z` versions are dispatched. Pre-release and dev tags
  (`1.6.1-rc0`, `X.Y.Z-dev`) skip the dispatch, because siren validates the
  version against `^X.Y.Z$` and unconditionally moves its
  `defaults.vectorstore` to the dispatched version. For the same reason,
  releasing a patch on an *older* series still moves `defaults.vectorstore`
  back to it — review the siren PR before merging in that case.
- `workflow_dispatch` returns no run id, so the job cannot report the siren
  run status. It logs the dispatched image IDs and a link to the siren
  workflow page in its job summary — verify there that the run succeeded and
  the PR appeared.

If the dispatch fails, or the siren run does, the cloud images do not need to
be rebuilt: re-run just the `trigger-siren` job, which re-reads the packer
manifest artifact of the same workflow run. A rejection caused by the token,
though, persists until the token is fixed, so re-running changes nothing.
The job's error says which call failed, and GitHub's message, logged just
above it, says whether the token is at fault:

- `Bad credentials` — the token has expired or been revoked, or the secret
  holds something that is not a token.
- `Not Found` while reading siren's default branch — the token does not have
  `scylladb/siren` among its repositories, or the organization has not
  approved it.
- `Resource not accessible by personal access token` while starting the
  workflow — the token lacks `Actions: read and write`.

Fix the token in the organization's settings, then re-run `trigger-siren`.
Any other message need not be the token's fault: siren may have renamed the
workflow or its inputs, or GitHub may be having an outage, in which case a
plain re-run can help.

To trigger siren by hand instead, start the same workflow the same way:
[add-vectorstore-ver](https://github.com/scylladb/siren/actions/workflows/add-vectorstore-ver.yml)
via `workflow_dispatch`, copying the version, the bare `us-east-1` amd64 and
arm64 AMI IDs, and the GCP image name from the packer manifest. For a release
started by publishing a GitHub release, the manifest is attached to the
release as the `vector-store-cloud-images-X.Y.Z.json` asset; a release
started via `workflow_dispatch` uploads no release asset, so download the
`vector-store-cloud-images-X.Y.Z` workflow artifact from the run page
instead. In the manifest the AWS `artifact_id` is a comma-separated
`region:ami` list — use the AMI from the `us-east-1` entry.

### Prerequisites

- Docker; qemu support is needed only when building a non-native
  architecture (see `scripts/prepare-docker-qemu`)
- Docker `buildx` plugin, used by `scripts/upload-dockers` to assemble the
  multi-arch tag. It is packaged separately from the docker engine, as
  `docker-buildx-plugin` in Docker's own repositories and `docker-buildx` in
  Debian/Ubuntu
- `jq` installed (used by `scripts/upload-dockers`)
- `cargo-cyclonedx` installed (`cargo install cargo-cyclonedx`)
- `syft` installed (see https://github.com/anchore/syft)
- `gh` CLI installed and authenticated (see https://cli.github.com/)

## Releasing docker toolchain image

Building release of vector-store requires a docker image with the toolchain.
The image is built from `scripts/Dockerfile.toolchain` and pushed to GitHub
registry as
`ghcr.io/scylladb/vector-store-toolchain:$toolchain_version-$rust_version-$TARGETARCH`.

The `$toolchain_version` is defined in `scripts/build-docker-toolchain` and
`$rust_version` is defined in `rust-toolchain.toml`. When CI processes PRs the
image is built when versions differ from master and pulled otherwise. If PR
with the versions changed is merged, the image must be built and pushed to the
registry by using workflow `release-toolchain.yaml`.

When we change rust version we must release the toolchain. When we change
`Dockerfile.toolchain` or `build-docker-toolchain` script in a way that affects
the image, we must bump the `$toolchain_version` in `build-docker-toolchain`
and release the toolchain.
