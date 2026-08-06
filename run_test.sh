#!/bin/bash
if [ -f .env ]; then
    set -a && source .env && set +a
fi

: "${NEO4J_USER:?Set NEO4J_USER in .env or environment}"
: "${NEO4J_PASSWORD:?Set NEO4J_PASSWORD in .env or environment}"
: "${NEO4J_URI:?Set NEO4J_URI in .env or environment}"

python -m unittest
