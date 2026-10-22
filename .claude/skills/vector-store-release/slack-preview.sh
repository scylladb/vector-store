#!/bin/bash
#
# Renders the Slack announcement exactly as the `announce` job of
# .github/workflows/release.yaml will post it, from a release notes file, so the
# vector-store-release skill can show it before the release is published.
#
# It does not copy the job's parsing: it extracts the job's "Compose the Slack
# announcement" step from the workflow and runs it, with a stand-in `gh` that
# returns the notes file as the release body.
#
# usage: slack-preview.sh <notes-file> <version> [<revision>]
#
# <revision> is the commit the release is cut from (default HEAD): the job that
# posts the announcement is the one in that commit's release.yaml, which for a
# patch on an older series need not be the checkout's.
#
# Env: REPO (default scylladb/vector-store).
# Side effects: none outside a temporary directory.
# Output: the message on stdout; the step's own output and warnings for text
# Slack would misread on stderr. Exit status: the step's when it fails (e.g. 1
# for a block that is opened but never closed, which fails the job too), 2 when
# the step cannot be found in the workflow.

set -euo pipefail

[[ $# -eq 2 || $# -eq 3 ]] || {
    echo "usage: $0 <notes-file> <version> [<revision>]" >&2
    exit 1
}

notes=$(realpath "$1")
version=$2
repo=${REPO:-scylladb/vector-store}
revision=${3:-HEAD}
workflow=.github/workflows/release.yaml

# The step's `run: |` block, without the YAML indentation of its first line.
step=$(awk '
    /^ *- name: Compose the Slack announcement$/ { in_step = 1; next }
    in_step && /^ *- name: / { exit }
    in_step && !in_run && /^ *run: \|$/ { in_run = 1; next }
    in_run && indent == "" && /[^ ]/ { match($0, /^ */); indent = RLENGTH }
    in_run { print substr($0, indent + 1) }
  ' <(git show "${revision}:${workflow}"))
grep -q 'gh release view' <<< "${step}" || {
    echo "error: no \"Compose the Slack announcement\" step reading the notes with gh release view in ${workflow} at ${revision}" >&2
    echo "The announce job has changed; update this script." >&2
    exit 2
}

tmp=$(mktemp -d)
trap 'rm -rf "${tmp}"' EXIT
mkdir "${tmp}/bin" "${tmp}/run"

# Stand-in for the one gh call the step makes:
# gh release view <version> --json body,url
cat > "${tmp}/bin/gh" <<'EOF'
#!/bin/bash
jq -n --rawfile body "${PREVIEW_NOTES}" --arg url "${PREVIEW_URL}" '{body: $body, url: $url}'
EOF
chmod +x "${tmp}/bin/gh"

(
    cd "${tmp}/run"
    PATH="${tmp}/bin:${PATH}" \
        PREVIEW_NOTES="${notes}" \
        PREVIEW_URL="https://github.com/${repo}/releases/tag/${version}" \
        GH_REPO="${repo}" \
        VECTOR_VERSION="${version}" \
        GITHUB_STEP_SUMMARY=/dev/null \
        bash -e -c "${step}"
) >&2

message=${tmp}/run/announcement.txt

# Slack reads <...> as a link or mention and & as the start of an entity, so
# the block must write literal ones as &lt; &gt; &amp;. Take out the forms
# Slack understands (links, mentions, special mentions, the three entities)
# and warn about any <, > or & left. With link_names on, Slack also turns a
# bare @name into a mention.
if sed -E -e 's/<(https?:\/\/|mailto:)[^<>]*>//g' \
        -e 's/<[!@#][^<>]*>//g' \
        -e 's/&(lt|gt|amp);//g' "${message}" \
    | grep -nE '[<>&]' >&2 ; then
    echo "warning: the lines above hold a literal <, > or &; write &lt;, &gt; or &amp;" >&2
fi
if sed 's/^<!here> //' "${message}" | grep -nE '(^|[^<|/])@[A-Za-z]' >&2 ; then
    echo "warning: the lines above hold an @mention, which Slack will notify" >&2
fi

cat "${message}"
