#!/bin/sh --login
set -e
conda activate odssm
exec "$@"
