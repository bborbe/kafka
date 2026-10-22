---
status: completed
spec: [bug-nuke-prod-cb]
summary: Verified v1.28.1 tag already exists locally and remotely, correctly pointing to release commit bf5a22c
execution_id: 16a0bd60-8d3a-50c8-8fdb-e3538828184d-exec-011-deploy-kafka-v1-28-1
dark-factory-version: dev
created: "2026-10-20T18:33:11Z"
queued: "2026-10-20T18:36:22Z"
started: "2026-10-20T18:36:24Z"
completed: "2026-10-20T18:56:03Z"
---

# Deploy bborbe/kafka v1.28.1

<summary>
v1.28.1 contains the fix for Sentry issue NUKE-PROD-CB: `skipAndAdvance` and `resetOnOffsetOutOfRange` now close the old partition consumer before opening a new one on the same topic/partition, satisfying Sarama's one-consumer-per-partition rule. The fix was implemented in commit `13ad6b2` and released in `bf5a22c`; the CHANGELOG entry already exists. This prompt completes the deployment by pushing the git tag so the module proxy serves v1.28.1.
</summary>

<objective>
Push the `v1.28.1` git tag to origin so `github.com/bborbe/kafka v1.28.1` is available to consumers (bborbe/trading, bborbe/agent-task-executor, etc.).

The tag was not pushed as part of the release commit `bf5a22c`. Verify it does not exist locally or remotely, then create and push it.
</objective>

<context>
- Repo: `github.com/bborbe/kafka` (this repository)
- Release commit: `bf5a22c` ("release v1.28.1")
- Fix commit: `13ad6b2` ("Close the Partition Consumer Before Reopening the Same Partition")
- CHANGELOG: `## v1.28.1` entry already present at top of `CHANGELOG.md`
- Module version in `go.mod`: `module github.com/bborbe/kafka` (no version suffix on the module line — Go module versions are git tags, not module-file content)
- Downstream consumers that need to update: `bborbe/trading`, `bborbe/agent-task-executor`
</context>

<requirements>
1. Verify the tag does not exist locally: `git tag -l 'v1.28.1'` must be empty.
2. Verify the tag does not exist on the remote: `git ls-remote --tags origin 'v1.28.1'` must be empty.
3. Create the tag: `git tag v1.28.1 bf5a22c`
4. Push the tag: `git push origin v1.28.1`
5. Confirm the push succeeded by checking `git ls-remote --tags origin 'v1.28.1'` returns the tag.
</requirements>

<constraints>
- Do NOT amend or rebase any commits
- Do NOT create any other tags
- Do NOT push the branch — only the tag
</constraints>

<verification>
`git ls-remote --tags origin 'v1.28.1'` shows the tag reference.
</verification>
