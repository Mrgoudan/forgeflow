#!/usr/bin/env bash
# Genericity gate: the platform core must never name a language, a project,
# a machine path, or a specific model. Run in CI / pre-commit; exit 1 on leak.
# Packs and vault are exempt by definition; forge adapters may name forges.
set -u
CORE="forgeflow"
FAIL=0

check() {  # check <pattern> <description> [extra grep args...]
    local pattern="$1" desc="$2"; shift 2
    local hits
    hits=$(grep -rniE "$pattern" "$CORE" --include='*.py' "$@" 2>/dev/null)
    if [ -n "$hits" ]; then
        echo "GENERICITY LEAK ($desc):"
        echo "$hits" | head -10
        echo
        FAIL=1
    fi
}

check '\bbsc\b|bisheng|\.cbs\b|\bclang\b|\bllvm\b|\bninja\b' "project/toolchain names in core" --exclude-dir=forge
check '/home/[a-z]' "absolute user paths in core"
check '\bopus\b|\bsonnet\b|\bqwen\b|glm-|gpt-' "model names in core"
check 'gitcode|gitee|github' "forge names outside forge/" --exclude-dir=forge

if [ "$FAIL" -eq 0 ]; then echo "core is generic: OK"; fi
exit $FAIL
