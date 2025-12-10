#!/bin/sh -e

# Example call for N Chile:

cat <<EOF > whitelist.txt
C
C1
WA
GT.LPAZ
GE.SALTA
GE.TCA
EOF

scexec scoctoloc \
	--debug \
	--center-latlon -22,-70 \
	--pick-authors 'dlpicker' \

##	--output-schedule 2,5
##	--whitelist whitelist.txt \
##	--output-xml output.xml
