# osm2obf

Produce `.obf` from `.osm` or PostgreSQL+PostGIS database, possibly province or country-sized.

**Experimental**


Uses
[OsmAndMapCreator](https://wiki.openstreetmap.org/wiki/OsmAndMapCreator) for all `obf` processing.


The main goal is to produce a `.obf` file without crashing. This can be a challenge on low-RAM
systems and that's why processing is usually done one-by-one to not put too much pressure on RAM
(instead of opting for performance by running everything in parallel).


For big geographical extracts, the `obf` file format has a 2GB hard max filesize limit.
To extract a country, this script will output multiple < 2GB `obf`s as north-south stripes.
If you want more meaningful sub-extracts, please run multiple times with corresponding provinces.
* **Planned** handle the "multiple runs" part automagically.
* **Planned** maybe take in a list of geojson files, for making the splits along them...


# Requirements

## Mode: `osm` input file


reading in an `osm` file directly, with
[osmium](https://osmcode.org/osmium-tool/)
and all of its supported input formats: `osm`,`osm.pbf`,`osm.bz2`,...

* `osmium`

## Mode: PostgreSQL+PostGIS OpenStreetMap database

* [pgsql2osm](https://github.com/feludwig/pgsql2osm)
* database `SELECT` permissions


# Usage

First, download OsmAndMapCreator
```
wget 'https://download.osmand.net/latest-night-build/OsmAndMapCreator-main.zip'
mkdir osmanmapcreator/
unzip OsmAndMapCreator-main.zip -d osmanmapcreator/
```
Then run (database mode)
```
python3 -u osm2obf.py 'dbname=gis' /absolute/path/to/osm2obf/osmanmapcreator {osm_rel_id} {output_obf_prefix}
```
Then run (file mode)
```
python3 -u osm2obf.py 'dbname=gis' /absolute/path/to/osm2obf/osmanmapcreator {input.osm} {output_obf_prefix}
```
Where `{output_obf_prefix}`'s directory will be used as the temporary working directory, and the output
`obf` will just be `{output_obf_prefix}.obf` IF it is smaller than 2GB (else there will be multiple `obf`s).

