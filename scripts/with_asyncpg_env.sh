#!/usr/bin/env bash
set -euo pipefail

normalize_asyncpg_url() {
  local value="$1"

  if [[ "$value" == postgresql+asyncpg://* ]]; then
    printf '%s' "$value"
    return
  fi

  if [[ "$value" == postgresql://* ]]; then
    printf 'postgresql+asyncpg://%s' "${value#postgresql://}"
    return
  fi

  if [[ "$value" == postgres://* ]]; then
    printf 'postgresql+asyncpg://%s' "${value#postgres://}"
    return
  fi

  printf '%s' "$value"
}

if [[ -n "${DATABASE_URL:-}" ]]; then
  export DATABASE_URL
  DATABASE_URL="$(normalize_asyncpg_url "$DATABASE_URL")"
fi

if [[ -n "${MIRROR_DATABASE_URL:-}" ]]; then
  export MIRROR_DATABASE_URL
  MIRROR_DATABASE_URL="$(normalize_asyncpg_url "$MIRROR_DATABASE_URL")"
fi

exec "$@"
