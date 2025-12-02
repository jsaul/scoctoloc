#!/bin/sh -e

db="mysql://YOURDATABASE"

eventid=gfz2024ofhj

# Generate inventory
# scexec scxmldump -I -d "$db" -o inventory.xml --debug
# Generate input XML
# scexec scxmldump -E $eventid -pPAMF -d $db -o $eventid.xml --debug

input="$eventid.xml"

seiscomp exec scoctoloc \
	--inventory-xml inventory.xml \
	--input-xml $input \
	--pick-authors 'dlpicker' \
	--whitelist whitelist.txt \
	--center-latlon -21,-68 \
	--max-distance 500 \
	--max-depth 350 \
	--output-xml output.xml \
	--debug
