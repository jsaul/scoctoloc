#!/bin/sh -e

# Example call for Vogtland region:

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

scoctoloc
	--whitelist whitelist.txt \
	--center-latlon 50.3,12.4 \
	--model-const 6.4,3.7,2.4 \
	--max-distance 200 --max-depth 20 \
	--debug
