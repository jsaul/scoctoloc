#!/bin/sh -e

# Example script for running scoctoloc on picks from the SeisComP database
# within a certain time span. Resulting origins are written to an XML file.
#
# This script runs scoctoloc in playback mode, i.e. simulating real-time
# behaviour.

t1="2025-12-01T00:00:00Z" t2="2026-01-01T00:00:00Z"

scexec scoctoloc \
	-u "" \
	--whitelist whitelist.txt \
	--center-latlon -22,-70 \
	--max-distance 400 --max-depth 350 \
	--pick-authors 'dlpicker' \
 	--start-time "$t1" \
	--end-time   "$t2" \
	--playback \
	--output-xml output.xml \
	--debug
