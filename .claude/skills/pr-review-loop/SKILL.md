---
name: pr-review-loop
description: Drive a pull request through repeated AI code review — re-trigger CodeRabbit and GitHub Copilot, wait for their comments, reply to every comment, fix the ones that are valid, and repeat until both reviewers are quiet or 5 rounds are spent. Use this whenever the user asks to re-trigger, re-run, or re-request review from CodeRabbit and/or Copilot, to loop or iterate on AI/bot review comments, to "get the bots off my PR", to address CodeRabbit or Copilot feedback, or to clean up a PR until the automated reviewers stop reporting findings — even if they do not name the bots or the word "loop".
---

# AI reviewer loop

Iterates CodeRabbit and Copilot review on one pull request until neither has
anything actionable left, capped at **5 rounds**. Each round is: trigger both
reviewers → wait → triage every new comment → fix or rebut → validate → push.

Fixes must follow this repo's rules: `CLAUDE.md`, `CONTRIBUTING.md`
(pre-review checklist, commit subject `module: changes`, Jira refs), and
`docs/rust_instructions.md`. A reviewer suggestion that conflicts with those
documents is **not** valid — rebut it, do not implement it.

## Setup (once)

1. Resolve `OWNER`/`REPO` — the repository the **PR lives in**, which is its
   base repo, and on a fork checkout is not the remote you push to. An API call
   aimed at the wrong repo either 404s or, worse, hits an unrelated PR that
   happens to share the branch name. Do not assume which remote is which:
   `origin` is the fork in one contributor's checkout and the base repo in
   another's.

   Do not derive it from the remotes. Neither a remote named `upstream` nor the
   fork network's root is the answer — in a chain A → B → C, a C → B PR lives
   in B while `.source` names A. **Find the PR first, then read the repository
   off the PR**, which is the only authoritative answer:

   ```sh
   # A full PR URL identifies the repo directly; a bare number does not, since
   # PR numbers are unique only within a repository — pair it with a repo the
   # user named, or resolve it by searching the head branch as below. Each hit
   # carries the repo the PR actually lives in.
   gh search prs --state open "head:$(git branch --show-current)" \
     --json number,repository --jq '.[] | "\(.repository.nameWithOwner)#\(.number)"'

   # Then confirm the pairing before acting on it:
   gh api "repos/$OWNER/$REPO/pulls/$PR" --jq \
     '{base:.base.repo.full_name, base_ref:.base.ref, head:.head.repo.full_name, head_ref:.head.ref}'
   ```

   `base` is `$OWNER/$REPO` for every later call, `base_ref` is the rebase
   target in step 5, and `head`/`head_ref` must match the checkout you are
   standing in — a branch name can exist in several repos of a fork network.
   Stop and ask if the search returns no open PR, or more than one you cannot
   disambiguate.
2. Read the PR body and diff (`pull_request_read` with `get` and `get_diff`)
   so you can judge comments on their merits rather than pattern-matching.
3. Set `ROUND=1`.

Prefer the GitHub MCP tools (`mcp__github__*`). Where they fall short, use
`gh api`; the recipes below give both.

Scope every `gh` call to `-R "$OWNER/$REPO"` (or spell out the full
`repos/$OWNER/$REPO/...` path). Unscoped, `gh` picks its own default repository,
which is whatever the checkout happens to call `origin` — exactly the mistake
step 1 of the setup exists to avoid.

For git, take the push remote from the branch itself rather than hardcoding
`origin`, and check it really points at the PR's head repo:

```sh
PUSH_REMOTE=$(git config "branch.$(git branch --show-current).remote" || echo origin)
git remote get-url "$PUSH_REMOTE"   # must match setup's head
```

## Each round

### 1. Mark the boundary, then trigger both reviewers

Record the boundary (current UTC time, `date -u +%Y-%m-%dT%H:%M:%SZ`) and the
head SHA **before** triggering. Everything newer than the boundary is this
round's feedback; everything older was already handled.

Keep that boundary **per reviewer** (`CR_SINCE`, `CP_SINCE`) and advance a
reviewer's boundary only once that reviewer has answered. A round that timed out
leaves the silent reviewer's boundary where it was, so a reply that lands
between the timeout and the next trigger is still newer than *its* boundary and
gets triaged. A single shared `SINCE` loses exactly that reply. Regardless of
timestamps, sweep unresolved threads for anything without an outcome before
declaring a round done — timestamps order work, they do not track it.

**CodeRabbit** — post a PR comment with `add_issue_comment`
(`gh pr comment "$PR" -R "$OWNER/$REPO" --body ...`):

- `@coderabbitai full review` on round 1, and on any later round where the head
  SHA has not moved — CodeRabbit skips a plain `review` when there are no new
  commits to look at.
- `@coderabbitai review` on later rounds after you have pushed fixes, for an
  incremental pass over the new commits.

**Copilot** — `request_copilot_review`, or
`gh api -X POST "repos/$OWNER/$REPO/pulls/$PR/requested_reviewers" -f "reviewers[]=copilot-pull-request-reviewer[bot]"`.

If either call fails, or a reviewer answers that it will not review now
(422/403, app not installed, Copilot review not enabled for the repo,
CodeRabbit replying `Review limit reached` on a rate-limited plan), that
reviewer is unavailable: say so once, set its
`CR_UNAVAILABLE`/`CP_UNAVAILABLE` flag for step 2, and keep going with the
other. If both are unavailable, stop and report — do not spin.

Confirm a Copilot request actually stuck: the POST returns 200 even when the
reviewer is dropped, so re-read `requested_reviewers` afterwards. On a **draft**
PR neither bot reviews on its own — CodeRabbit answers with "Draft PR not
reviewed" and Copilot silently drops the request — so an explicit
`@coderabbitai full review` is required, and Copilot may not answer at all until
the PR is marked ready. Do not mark it ready yourself; that is the author's call.

### 2. Wait for the reviews to land

Both bots take minutes, not seconds. Never block on a foreground `sleep`; poll
in the background with `Monitor` and keep the deadline bounded.

A reviewer step 1 marked unavailable starts already satisfied, so the loop is
never held to a signal that will not arrive. CodeRabbit edits one summary
comment in place and its first edit only says "review in progress" — treat it
as finished when that comment has been touched this round and no longer carries
the in-progress marker, or when it submits a review:

```sh
END=$(( $(date +%s) + 900 ))
cr=${CR_UNAVAILABLE:-0}; cp=${CP_UNAVAILABLE:-0}
while :; do
  if [ "$cr" -eq 0 ]; then
    body=$(gh api "repos/$OWNER/$REPO/issues/$PR/comments" --paginate --jq \
             ".[] | select(.user.login==\"coderabbitai[bot]\")
                  | select(.updated_at > \"$CR_SINCE\") | .body" 2>/dev/null)
    rev=$(gh api "repos/$OWNER/$REPO/pulls/$PR/reviews" --paginate --jq \
            ".[] | select(.submitted_at > \"$CR_SINCE\") | .user.login" 2>/dev/null \
          | grep -c coderabbitai)
    if [ -n "$body" ] && ! grep -q 'review in progress by coderabbit' <<<"$body"; then
      echo "coderabbit finished (summary settled)"; cr=1
    elif [ "${rev:-0}" -gt 0 ]; then
      echo "coderabbit finished (review submitted)"; cr=1
    fi
  fi
  if [ "$cp" -eq 0 ]; then
    gh api "repos/$OWNER/$REPO/pulls/$PR/reviews" --paginate --jq \
      ".[] | select(.submitted_at > \"$CP_SINCE\") | .user.login" 2>/dev/null \
      | grep -qi '^copilot' && { echo "copilot responded"; cp=1; }
  fi
  [ "$cr" -eq 1 ] && [ "$cp" -eq 1 ] && { echo "both responded"; break; }
  [ "$(date +%s)" -ge $END ] && { echo "timeout coderabbit=$cr copilot=$cp"; break; }
  sleep 30
done
```

Without `gh`, call `subscribe_pr_activity` for the PR and schedule a re-check
with `send_later` (~5 minutes) instead of polling. On timeout, proceed with
whatever arrived and note which reviewer never answered.

### 3. Triage every new comment

Collect this round's feedback: `pull_request_read` with `get_review_comments`
(review threads, with their GraphQL thread IDs), `get_reviews` (Copilot's
review body), and `get_comments` (CodeRabbit's summary comment).

Ignore non-actionable noise: CodeRabbit's summary/walkthrough, its collapsed
"nitpick"/"outside diff range" blocks it marked non-blocking, Copilot's
"reviewed N files" preamble, and anything you already addressed in an earlier
round. Also skip comments authored by you.

For each remaining comment, decide and then act — every comment gets a visible
outcome, so a reviewer can see what happened:

- **Valid** → fix the code, then reply on the thread
  (`add_reply_to_pull_request_comment`) saying what you changed, and resolve
  the thread (`resolve_review_thread`) once the fix is pushed.
- **Valid but out of this PR's scope** → reply saying so and why; leave the
  thread open. Do not widen the PR (`CONTRIBUTING.md`: one logical change).
- **Wrong, or against repo convention** → reply with the concrete reason,
  citing the guideline or the code that makes it wrong. Addressing
  `@coderabbitai` in the reply gets you an answer you can argue with; Copilot
  does not converse, so one clear rebuttal is enough. Do not resolve a thread
  just to silence it.

Human reviewers comment on the same threads as the bots. Give their comments
the same visible outcome — fix or answer — but leave their threads **open** for
them to close, and do not argue a maintainer's call the way you would a bot's.
A human comment that lands between one round's triage and the next round's
boundary is precisely what the unresolved-thread sweep in step 1 is there to
catch.

Never disable, skip, or weaken a test to satisfy a comment.

### 4. Validate before pushing

Run what CI runs — warnings are errors:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -Dwarnings
cargo clippy --features slow-test-hooks --workspace --all-targets -- -Dwarnings
cargo test --features dev-tools,slow-test-hooks --workspace --all-targets
```

The two feature-gated variants are not optional: `.github/workflows/rust.yml`
lints with `slow-test-hooks` and runs the tests with `dev-tools,slow-test-hooks`,
so the plain commands leave the feature-gated code and its tests unbuilt. When a
change touches dependencies or the crate manifest, `cargo deny check` and
`cargo machete` are CI jobs too.

Regenerate `api/openapi.json` with `cargo openapi` if the REST API changed;
never hand-edit it. A push that turns CI red costs a whole round, so push only
once these come back clean.

`.github/workflows/validator.yml` runs the end-to-end validator harness, which
CI also gates on. Do **not** run it once per round: locally it builds two
release binaries and drives a real ScyllaDB container, so it costs far more than
a round of review does. Run it once before the final push of a change that
touches runtime behaviour — see the Testing section of `CONTRIBUTING.md` — and
skip it entirely for changes that cannot affect it (documentation, tooling, CI
config).

### 5. Commit and push

Commit per `CONTRIBUTING.md`: subject `module: changes`, a body explaining
*why*, and `Fixes:`/`Refs: VECTOR-<n>` where the PR already carries one.

- Branch is yours and the PR is a patch series → fold each fix into the commit
  it belongs to so every patch stays individually correct, then force-push with
  `--force-with-lease`. Name the target commit and rebase from the PR's base,
  not from the branch's own upstream — with no upstream argument the range
  starts after the commit you are trying to amend, and the `fixup!` survives
  unsquashed:

  ```sh
  git commit --fixup "$TARGET_SHA"
  # Resolve the base branch from the PR and fetch it from the base repo — on a
  # fork the branch may have no local tracking ref, and the fork's own copy of
  # it is not the PR's base.
  PR_BASE=$(gh api "repos/$OWNER/$REPO/pulls/$PR" --jq .base.ref)   # setup's base_ref
  git fetch "https://github.com/$OWNER/$REPO.git" "$PR_BASE"
  GIT_SEQUENCE_EDITOR=true git rebase --autosquash "$(git merge-base HEAD FETCH_HEAD)"
  git push --force-with-lease "$PUSH_REMOTE" "$(git branch --show-current)"
  ```
- Otherwise, or if anyone else may have the branch checked out → add a plain
  follow-up commit and push with `git push "$PUSH_REMOTE" <branch>` (add `-u`
  only when the branch has no upstream yet). Never rewrite history on a branch
  that is not yours.

Each path carries its own push: `--force-with-lease` after a fold, a plain push
after a follow-up commit. Do not push a rewritten branch without the lease, and
do not expect a plain push to land one — it is rejected as non-fast-forward.

### 6. Decide whether to loop

Stop when **both** reviewers have gone quiet:

- CodeRabbit reports `Actionable comments posted: 0`, and
- Copilot's review adds no new comments,

with no unaddressed threads left. A reviewer that step 1 found unavailable
counts as quiet here too, exactly as it does in step 2 — otherwise a PR one bot
cannot review never converges, and burns all five rounds for nothing.

If anything is still open, `ROUND=$((ROUND+1))` and go back to step 1 — up to
`ROUND=5`.

Nothing pushed this round and the same comments coming back means you are not
converging: stop early, and report what is disputed rather than burning rounds.

## Final report

Tell the user, in a few lines: rounds used, how many comments came from each
reviewer, what you fixed, what you pushed back on and why, and anything still
open (including a reviewer that never responded). If CI is still red or the PR
has a merge conflict, say so plainly — the loop is not done until the PR is
green.

Keep posted comments short and factual. End each comment you post on GitHub
with the attribution footer your environment requires, so reviewers can tell
which replies were written by Claude.
