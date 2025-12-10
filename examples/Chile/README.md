# scoctoloc example scripts

This folder contains a few scripts to illustrate the usage and invocation of scoctoloc.
The test region here is northern Chile.
Our network consists of stations within 400 km around the coordinate 22°S/70°W.


## run-database-offline.sh

This is a common use case: Many picks in a database and the events shall be associated and relocated.
We load the picks from the SeisComP database within a certain time span defined by `--start-time` and `--end-time`.
Processing is performed only for the picks from streams matching the patterns in `whitelist.txt` and for which the author is `dlpicker`.
Results are written to `output.xml`.
Note that the output also contains the input picks.
The `output.xml` can be viewed in scolv or processed further.


## run-database-playback.sh

Like `run-database-offline.sh` but running as playback.
This means that picks are processed as they are received like in a real-time processing, resulting in several to many origins per event as more picks come in.
