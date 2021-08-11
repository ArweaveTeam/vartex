#! /bin/bash
set -e
export PATH=$PATH:$HOME/.local/bin

cp .env.koii .env
docker build -f Dockerfile -t koii_vartex_gateway:latest .
