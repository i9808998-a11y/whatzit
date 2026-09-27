#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if [ ! -f .env ]; then cp .env.example .env; fi
set -a
. ./.env
set +a
node server.js
