#!/bin/bash
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=Rit@2315
#export NEO4J_AURA_PASSWORD=Qff6i4GfyvmnDpCBwD6_SiAfQLsRQYWkOydHysS-Gjw
export NEO4J_URI=bolt://localhost:7687

python -m unittest
