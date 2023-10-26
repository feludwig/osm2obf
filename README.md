# osm2obf

Produce `.obf` from `.osm` or PostgreSQL+PostGIS database, possibly country-sized or bigger.


The main goal is to produce a `.obf` file without crashing. This can be a challenge on low-RAM
systems and processing is usually done one-by-one to not put too much pressure on RAM
(instead of opting for performance by running everything in parallel).

**Experimental**

# Requirements

## `osm` input file


**Unsupported** : reading in osm directly, for now only extracts from database.

* `osmium`

## PostgreSQL+PostGIS OpenStreetMap database

* [https://github.com/feludwig/pgsql2osm](pgsql2osm)
* database `SELECT` permissions

* still need to translate mkobf.sh to python


