#! /bin/bash
set -e
export PATH=$PATH:$HOME/.local/bin

docker build -f Dockerfile -t koii_vartex_gateway:latest .
