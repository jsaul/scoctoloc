#!/bin/sh -e

# Database playback for a few hours of Vogtland swarm events

t1="2025-12-09T20:00:00Z" t2="2025-12-10T12:00:00Z"

cat <<EOF > whitelist.txt
CZ
GR
SX
GE.FALKS
GE.MORC
BW
M1
TH
EOF

scoctoloc \
	-u "" \
	--whitelist whitelist.txt \
	--center-latlon 50.3,12.4 \
	--model-const 6.4,3.7,2.4 \
	--max-distance 200 --max-depth 20 \
 	--start-time "$t1" \
	--end-time   "$t2" \
 	--playback \
	--output-xml output.xml \
	--debug
