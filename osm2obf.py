#!/usr/bin/python3

import psycopg2
import sys
import subprocess
import bz2
import os
import datetime
import typing

#relative to self location
sys.path.extend(['../pgsql2osm','pgsql2osm'])
import pgsql2osm


def get_x_ranges(areas) :
    #only work with integers, python gets confused with floats
    start_bx=-180*100
    curr_bx=start_bx
    bx_step=10
    curr_area=0.0
    finished=False

    while not finished :
        while curr_area<2.0 :
            curr_bx+=bx_step
            if curr_bx>=180*100 :
                finished=True
                break
            if curr_bx not in areas :
                continue
            curr_area+=areas[curr_bx]
            #check nonzero area to which we step back...
            if curr_area>2.5 and curr_area>areas[curr_bx]:
                #step back
                stepped_back=True
                curr_area-=areas[curr_bx]
                curr_bx-=bx_step
                break
        start,end=(start_bx*1e-2,curr_bx*1e-2)
        if start>-179.98 :
            start-=0.001
        if end<179.98 :
            end+=0.001
        yield (round(start,8),round(end,8))
        start_bx=curr_bx
        curr_area=0.0

def run_pgsql2osm(m:pgsql2osm.ModuleSettings,st_x,en_x,outfile_pfx) :
    st_x_s=str(st_x).replace('.','_')
    en_x_s=str(en_x).replace('.','_')
    outfile=f'{outfile_pfx}_{st_x_s}-{en_x_s}'.replace('.','_')
    outfile+='.osm.bz2'
    print('pgsql2osm',f"--bbox={st_x},-89,{en_x},89",'| bzip2 >',outfile,'...')
    with bz2.open(outfile,'wb') as f:
        m.bounds_box=f'{st_x},-89,{en_x},89'
        m.out_file=f
        m.main()
    print(round(os.path.getsize(outfile)/1e6,1),'MB of bzip2 output',file=sys.stderr)
    return outfile

def multi_osm_to_obf(access:psycopg2.extensions.connection,
        areas:typing.Dict[int,float],
        output_prefix:str,osm_rel_id:int)->typing.Iterator[str] :

    m=pgsql2osm.ModuleSettings(
        bounds_rel_id=osm_rel_id,
        get_lonlat_binary='/home/user/src/osm2pgsql/build/get_lonlat',
        nodes_file='/mnt/dbp/maps/planet.bin.nodes',
        access=access)

    slices=list(get_x_ranges(areas))
    print('getting',len(slices),'slices')
    for st_x,en_x in slices :
        stt=datetime.datetime.now()
        out_fn=run_pgsql2osm(m,st_x,en_x,output_prefix)
        dt=datetime.datetime.now()-stt
        print('extracted',out_fn,'in',dt)
        yield out_fn #after printing

def calculate_areas(c:psycopg2.extensions.cursor)->typing.Dict[int,float] :
    print('calculating area split...')
    # ST_Intersects is much better, bbox && gets tripped up by -90 to +90 N/S extent
    # and says true much too often
    #print(c.mogrify(f"""WITH a AS (
    q="""WITH a AS (
        SELECT way AS a_3857 FROM planet_osm_polygon WHERE osm_id=-%s LIMIT 1),
    xs AS (SELECT generate_series(-180.0,180.0,0.1) AS x),
    sg AS (SELECT ST_Transform(
        ST_MakeEnvelope(xs.x,-89.999,xs.x+0.1,89.999,4326),3857
        ) AS a_3857,x FROM xs)
    SELECT ST_Area(ST_Intersection(sg.a_3857,a.a_3857)) AS a,(sg.x*100::int) AS x
    FROM sg,a WHERE ST_Intersects(sg.a_3857,a.a_3857)
        ORDER BY x"""
    split_size='0.1' # AS a str
    # take square-degrees areas
    q=f"""SELECT
            ST_Area(ST_Transform(
                    ST_Intersection((SELECT way FROM planet_osm_polygon WHERE osm_id=-%s),
                a_3857),4326)),(x*100)::int AS x
        FROM (SELECT ST_Transform(
                ST_MakeEnvelope(x,-89.999,x+{split_size},89.999,4326),3857) AS a_3857,x
            FROM generate_series(-180.0,180.0,{split_size}) AS x
        ) AS foo
        WHERE ST_Intersects(a_3857,(SELECT way FROM planet_osm_polygon WHERE osm_id=-%s));"""
    c.execute(c.mogrify(q,(osm_rel_id,osm_rel_id,)))
    areas={}
    for (area,x) in c.fetchall() :
        areas[x]=area

    print('got area split')
    return areas

"""
Usage:
    This script manages different programs together to produce a .obf file at the end.
    Input data is read from the database and converted to .osm.bz2 with pgsql2osm.py
    in stripes.
    Those stripes are then individually converted to .obf with the RAM-hungry
        process: do not paralellize unless you have >64GB RAM to spare
    And finally, all stripes are merged into one big .obf

TODO: add .osm* input option which internally calls osmium for --bbox extracts...
"""

dbaccess,osm_rel_id,output_prefix=sys.argv[1:]
osm_rel_id=int(osm_rel_id)


a=psycopg2.connect(dbaccess)

out_filenames=list(multi_osm_to_obf(a,calculate_areas(a.cursor()),output_prefix,osm_rel_id))

subprocess.check_call(['bash','/home/user/mkobf.sh',output_prefix+'.obf','10G',*out_filenames])

[os.remove(i) for i in out_filenames]
