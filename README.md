# osm2obf

Produce `.obf` from `.osm` or PostgreSQL+PostGIS database, possibly province or
small country-sized.


Using
[OsmAndMapCreator](https://wiki.openstreetmap.org/wiki/OsmAndMapCreator) for all `obf` processing.


The main goal is to produce a `.obf` file without crashing. This can be a challenge on low-RAM
systems and that's why processing is usually done one-by-one to not put too much pressure on RAM
(instead of opting for performance by running everything in parallel).


For big geographical extracts, the `.obf` file format does not allow files bigger than 2GB.
To extract a country, this script will output multiple < 2GB `obf`s as north-south stripes.
If you want more meaningful sub-extracts, please run multiple times with corresponding provinces.
* **Planned** handle the "multiple runs" part automagically.
* **Planned** maybe take in a list of geojson files, for making the splits along them...


# Usage

After cloning this repository,
```
git clone https://github.com/feludwig/osm2obf
cd osm2obf/
```
Download OsmAndMapCreator
```
wget 'https://download.osmand.net/latest-night-build/OsmAndMapCreator-main.zip'
mkdir osmandmapcreator/
unzip OsmAndMapCreator-main.zip -d osmandmapcreator/
```
Then run
* `postgres` (database) mode
```
python3 osm2obf.py postgres --dsn 'dbname=gis' -r {osm_rel_id} --output {output_file.obf}
```
* `osmium` (file) mode
```
python3 osm2obf.py osmium --dsn 'dbname=gis' --input {input.osm} --output {output_file.obf}
```
Where `{output_file.obf}`'s directory will be used as the temporary working directory, and the output
`obf` will just be `{output_file.obf}` IF it is smaller than 2GB (else there will be multiple `obf`s).


# Requirements

For all modes
* `java` for `OsmAndMapCreator`, instructions to [download](#usage).

## Mode: osmium


reading in a `.osm` file directly, with
[osmium](https://osmcode.org/osmium-tool/)
and all of its supported input formats: `.osm`,`.osm.pbf`,`.osm.bz2`,...

* `osmium`


## Mode: PostgreSQL+PostGIS OpenStreetMap database


* `pip install [pgsql2osm](https://github.com/feludwig/pgsql2osm)`
    - also requires `osm2pgsql` and planet nodesfile
* database `SELECT` permissions

