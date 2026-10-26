#!/usr/bin/env bash
# Waits until Vercel's commit status for $GITHUB_SHA is success, then writes
# the branch's Preview alias (which points at that branch's newest Preview)
# to $GITHUB_ENV as PREVIEW_URL. Vercel reports this repo's Previews as a
# commit status, not as GitHub deployments, so deployment_status never fires.
# Keep branch names short: Vercel shortens long aliases differently.
# $1, if given, is the branch whose alias to use instead of this one; it must
# be at this same commit (Vercel builds a commit once, under its first branch).
set -euo pipefail
branch="${1:-$GITHUB_REF_NAME}"
if [ "$branch" != "$GITHUB_REF_NAME" ]; then
  head=$(gh api "repos/$GITHUB_REPOSITORY/branches/$branch" -q .commit.sha)
  [ "$head" = "$GITHUB_SHA" ] || { echo "$branch is at ${head:0:7}, not ${GITHUB_SHA:0:7}" >&2; exit 1; }
fi
for i in $(seq 1 60); do
  state=$(gh api "repos/$GITHUB_REPOSITORY/commits/$GITHUB_SHA/status" \
    -q '[.statuses[] | select(.context == "Vercel")][0].state // "missing"')
  echo "Vercel status for ${GITHUB_SHA:0:7}: $state"
  case "$state" in
    success) break ;;
    failure | error) echo "Vercel build failed" >&2; exit 1 ;;
  esac
  [ "$i" = 60 ] && { echo "Vercel status not ready after 15 minutes" >&2; exit 1; }
  sleep 15
done
slug=$(printf '%s' "$branch" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9' '-')
echo "PREVIEW_URL=https://ibpl-game-logs-git-${slug}-ariel12091-8759.vercel.app" >> "$GITHUB_ENV"
