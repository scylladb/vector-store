# Releasing Vector Store

## TL;DR — cutting a release

Almost everything is automated. For a normal GA release `X.Y.Z`:

1. **Push an annotated tag** on the commit you want to release. Tag locally —
   creating the tag from the GitHub UI produces a *lightweight* tag, which the
   version machinery ignores (see [Versioning and tags](#versioning-and-tags)):

   ```bash
   git tag -a X.Y.Z -m "Release version X.Y.Z"
   git push origin X.Y.Z
   ```

2. **Publish a GitHub Release for that existing tag**: *Releases → Draft a new
   release*, pick `X.Y.Z` from the tag dropdown, write the release notes, leave
   *Set as a pre-release* **unchecked**, and press *Publish release*.

3. **Watch [Vector Store Release](https://github.com/scylladb/vector-store/actions/workflows/release.yaml).**
   Publishing starts it. It runs the full validator suite on amd64 and arm64,
   then pushes the DockerHub tags, builds the AWS/GCP cloud images, opens the
   version PR in siren, and announces the release on Slack. No manual step is
   needed for any of that.

4. **Attach the tarballs and SBOMs to the release.** This is the one part the
   workflow does *not* do — see [Attaching the tarballs and
   SBOMs](#attaching-the-tarballs-and-sboms-to-the-release).

That is the whole happy path. The rest of this document explains what each step
does, and how to do any of it by hand.

## Versioning and tags

`Cargo.toml` carries the placeholder `version = "0.0.0-dev"`. The real version
is derived from git at build time: `scripts/run-with-release-toolchain` runs
`git describe --dirty` and rewrites the placeholder in a temporary `Cargo.toml`
/ `Cargo.lock` mounted into the build container, so the source tree is never
modified.

**The tag must be annotated.** `git describe` without `--tags` only considers
annotated tags, so a lightweight tag is invisible to it and the build versions
itself from the *previous* annotated tag instead. This has happened before —
`1.8.1` was tagged through the GitHub UI:

```
$ git describe 1.8.1     # lightweight tag created in the GitHub UI
1.8.0-24-g36c47d7
$ git describe 1.10.0    # annotated tag pushed from a local checkout
1.10.0
```

GitHub cannot create annotated tags, which is why step 1 above pushes the tag
from a local checkout and step 2 only *points* the release at it.

Additional metadata in the tag is allowed — `X.Y.Z-rc0`, `X.Y.Z-dev` — but it
changes what the workflow does; see the trigger and siren caveats below.

## What the release workflow does

[`.github/workflows/release.yaml`](../.github/workflows/release.yaml) — *Vector
Store Release*.

### Triggers

- **`release: [released]`** — publishing a GitHub Release. Note this activity
  type does *not* fire for a **pre-release**: publishing a release with *Set as
  a pre-release* ticked runs nothing (un-ticking it later does fire the
  workflow). Draft releases do not fire it either.
- **`workflow_dispatch`** with a `VECTOR_VERSION` input (the git tag to build
  from) — for re-runs, and for building a tag without publishing a release.

Both paths set `VECTOR_VERSION`, and every job that checks out sources does so
at that tag rather than at the branch head.

### Jobs

| Job | What it does |
| --- | --- |
| `release-get-scylla-nightly-digest` | Pins ScyllaDB to the last **known-good** `scylla-nightly` digest recorded by the Daily workflow (`use-latest-scylla-nightly: false`), not to whatever `latest` currently is. |
| `release-build-validator`, `release-build-vector-store` | Build the validator and the `vector-store` binary with the release toolchain, natively on amd64 (`ubuntu-latest`) and arm64 (`ubuntu-24.04-arm`). |
| `release-get-validator-test-list`, `release-run-validator-tests` | Run the full validator suite, one job per test case × architecture. **The release is gated on this** — nothing is published if it fails. |
| `release-build-docker-archive-sbom` | Per architecture: builds the docker image, the `tar.gz`, and the Cargo + docker SBOMs, and uploads them as *workflow artifacts*. |
| `push-docker` | Pushes `scylladb/vector-store:X.Y.Z-amd64` and `-arm64` to DockerHub and combines them into the multi-arch `scylladb/vector-store:X.Y.Z`. |
| `build-cloud-images` | Packer-builds the AWS AMIs (amd64 + arm64) and the GCP image, and attaches the packer manifest to the release. |
| `trigger-siren` | Dispatches the version PR to scylladb/siren (GA versions only). |
| `announce` | Posts to the Slack `#release-announce` channel. |

### What ends up where

Automatically:

- **DockerHub** — `scylladb/vector-store:X.Y.Z` (multi-arch) plus the
  `X.Y.Z-amd64` / `X.Y.Z-arm64` per-arch tags.
- **AWS / GCP** — the AMIs and the GCE image.
- **GitHub Release assets** — only `vector-store-cloud-images-X.Y.Z.json`, the
  packer manifest. That step is guarded on the `release` trigger, so a
  `workflow_dispatch` run does not attach it.
- **Workflow artifacts** on the run page — the release tarballs
  (`vector-store-X.Y.Z-<arch>`), the SBOMs (`vector-store-sboms-X.Y.Z`), the
  saved docker images, and the packer manifest.
- **scylladb/siren** — the `versions.yaml` PR, for GA versions.
- **Slack** — the `#release-announce` message.

Manually, after the workflow is green:

- **GitHub Release assets** — the `tar.gz` archives and the SBOM files, see
  below.
- **Release notes** — written by hand when drafting the release.

## Attaching the tarballs and SBOMs to the release

The workflow builds the tarballs and SBOMs but only keeps them as workflow
artifacts; it never attaches them to the GitHub Release. Someone has to upload
them.

The cheapest way is to reuse what the workflow already built: from the run page
download the `vector-store-X.Y.Z-amd64`, `vector-store-X.Y.Z-arm64` and
`vector-store-sboms-X.Y.Z` artifacts, unpack them, and upload:

```bash
gh release upload X.Y.Z \
    vector-store-X.Y.Z-amd64.tar.gz \
    vector-store-X.Y.Z-arm64.tar.gz \
    vector-store-X.Y.Z.cdx.json \
    vector-store-docker-X.Y.Z-amd64.cdx.json \
    vector-store-docker-X.Y.Z-arm64.cdx.json \
    --clobber
```

Alternatively, build the release locally and let the script find the files:

```bash
git checkout X.Y.Z
./scripts/build-release
./scripts/upload-release
```

`scripts/upload-release` derives the version the same way the build does
(`git describe` on a clean tree, so check out the tag first), expects the
tarballs under `target/<arch>/release/` and both SBOM kinds in the repository
root, verifies each one is present, and uploads them with `gh release upload
--clobber`. `scripts/build-release` builds both architectures, so on a single
host this needs qemu for the non-native one — the artifact route above avoids
that.

## Re-running parts of a release

Individual jobs can be re-run from the workflow run page, and the pipeline is
split so that the expensive steps do not have to be repeated:

- `push-docker` consumes the saved docker images as artifacts, so it re-runs
  without rebuilding them.
- `trigger-siren` re-reads the packer manifest artifact of the same run, so a
  failed dispatch does not mean rebuilding the cloud images.

To build a tag without publishing a GitHub Release — a dry run, or a `X.Y.Z-dev`
build — start the workflow via *Run workflow* and pass the tag as
`VECTOR_VERSION`. Remember that such a run attaches no release asset, and that
`trigger-siren` skips any non-GA version.

## Registering the release in siren

The release workflow registers a GA release with siren automatically: after
`build-cloud-images` uploads the packer manifest, the `trigger-siren` job
extracts the `us-east-1` amd64/arm64 AMI IDs and the GCP image name from it
and sends a `vectorstore-release` `repository_dispatch` to scylladb/siren.
That starts siren's
[add-vectorstore-ver](https://github.com/scylladb/siren/actions/workflows/add-vectorstore-ver.yml)
workflow, which opens a `feat(vectorstore): add version X.Y.Z [automation]`
PR against siren's `info/version/versions.yaml`. No manual step is needed for
a GA release.

Two caveats:

- Only GA `X.Y.Z` versions are dispatched. Pre-release and dev tags
  (`1.6.1-rc0`, `X.Y.Z-dev`) skip the dispatch, because siren validates the
  version against `^X.Y.Z$` and unconditionally moves its
  `defaults.vectorstore` to the dispatched version. For the same reason,
  releasing a patch on an *older* series still moves `defaults.vectorstore`
  back to it — review the siren PR before merging in that case.
- `repository_dispatch` returns no run id, so the job cannot report the siren
  run status. It logs the dispatched image IDs and a link to the siren
  workflow page in its job summary — verify there that the run succeeded and
  the PR appeared.

If the dispatch fails, or the siren run does, the cloud images do not need to
be rebuilt: re-run just the `trigger-siren` job, which re-reads the packer
manifest artifact of the same workflow run. To trigger siren fully by hand
instead, start
[add-vectorstore-ver](https://github.com/scylladb/siren/actions/workflows/add-vectorstore-ver.yml)
via `workflow_dispatch`, copying the version, the bare `us-east-1` amd64 and
arm64 AMI IDs, and the GCP image name from the packer manifest. For a release
started by publishing a GitHub release, the manifest is attached to the
release as the `vector-store-cloud-images-X.Y.Z.json` asset; a release
started via `workflow_dispatch` uploads no release asset, so download the
`vector-store-cloud-images-X.Y.Z` workflow artifact from the run page
instead. In the manifest the AWS `artifact_id` is a comma-separated
`region:ami` list — use the AMI from the `us-east-1` entry.

## Building a release by hand

You should not need this for a normal release — it is here for debugging the
release pipeline, and for building artifacts outside CI.

The scripts are designed to be run from the root of the repository, on a clean
checkout of the version tag: `scripts/build-release` refuses to run with a dirty
working tree, because `git describe --dirty` would stamp the artifacts with a
`-dirty` version.

```bash
git checkout X.Y.Z
./scripts/build-release
```

This builds the binaries for both architectures, then the per-architecture
docker images (`scripts/build-dockers`), the `tar.gz` archives and the SBOMs
(`scripts/generate-archive-sbom`). Archives land in
`target/{amd64,arm64}/release`, SBOMs in the repository root. The script accepts
a subset of architectures as arguments, e.g. `./scripts/build-release arm64`.

Building an architecture other than the host's requires qemu plus docker support
for emulated builds; see
https://docs.docker.com/build/building/multi-platform/, and
`scripts/prepare-docker-qemu` to register the qemu handlers with the docker
engine. Building only the host's native architecture needs no qemu — that is how
the release workflow builds each architecture, natively on its own per-arch
runner.

The SBOMs are CycloneDX JSON:

- `vector-store-{version}.cdx.json` — Cargo dependencies, generated by
  `cargo-cyclonedx`
- `vector-store-docker-{version}-{arch}.cdx.json` — per-architecture docker
  image SBOM generated by `syft`, covering OS-level packages from the base image

### Pushing the docker images

No multi-platform docker image is built locally: the default docker driver
cannot store one in the classic image store. Instead the multi-arch tag is
assembled registry-side from the pushed per-arch images:

```bash
./scripts/upload-dockers
```

This pushes the per-arch images and then combines them into the multi-arch
tag with `docker buildx imagetools create`, so the tag lists exactly the two
architectures:

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

The release workflow's `push-docker` job does the same thing inline, on the
images built by the previous job.

### Prerequisites

Only needed for the manual path — CI installs its own tooling.

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

A rust toolchain on the host is not required:
`scripts/run-with-release-toolchain` runs cargo inside the
`rust:<channel>-bookworm` image matching `rust-toolchain.toml`.

## After the release

- Verify the siren `versions.yaml` PR appeared and looks right, then get it
  merged.
- Bump the pinned `scylladb/vector-store` image in
  `docs/examples/docker/docker-compose.yml` and
  `docker-compose-alternator.yml`, so the quick-start examples run the current
  release.
