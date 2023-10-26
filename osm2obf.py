#!/usr/bin/python3

import sys
import subprocess
import json
import bz2
import os
import datetime
import typing
import argparse


#relative to self location
sys.path.extend(['../pgsql2osm','pgsql2osm'])
try :
    import pgsql2osm
    available_pgsgl2osm=True
    psycopg2=pgsql2osm.psycopg2
except (ModuleNotFoundError,ImportError) :
    available_pgsgl2osm=False

bbox_t=typing.Tuple[float,float,float,float]


def get_stripes_by_area(areas)->typing.Iterator[bbox_t] :
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
        yield (round(start,8),-89,round(end,8),89)
        start_bx=curr_bx
        curr_area=0.0

def run_pgsql2osm(m:pgsql2osm.ModuleSettings,bbox:bbox_t,outfile_pfx:str) :
    st_x_s=str(bbox[0]).replace('.','_')
    en_x_s=str(bbox[2]).replace('.','_')
    outfile=f'{outfile_pfx}_{st_x_s}-{en_x_s}'.replace('.','_')
    outfile+='.osm.bz2'
    bbox_as_str=','.join(map(str,bbox))
    print('pgsql2osm','--bbox='+bbox_as_str,'| bzip2 >',outfile,'...')

    stt=datetime.datetime.now()
    with bz2.open(outfile,'wb') as f:
        m.bounds_box=bbox_as_str
        m.out_file=f
        m.main()

    dt=datetime.datetime.now()-stt
    print(round(os.path.getsize(outfile)/1e6,1),'MB for this bzip2 split in',dt)
    return outfile

def multi_osm_to_obf_osmium(bboxes:typing.Iterator[bbox_t],input_file:str,output_prefix:str)->typing.Iterator[str] :
    bboxes=list(bboxes) #collapse generator
    print('getting',len(bboxes),'bboxes')
    encountered_out_descrs={}
    for bbox in bboxes :
        out_descr=str(bbox[0]).replace('.','_')
        out_descr+='-'+str(bbox[2]).replace('.','_')
        if out_descr not in encountered_out_descrs :
            encountered_out_descrs[out_descr]=0
        else :
            encountered_out_descrs[out_descr]+=1
        int_charcode=b'a'[0]+encountered_out_descrs[out_descr] # 'a'+2=='c'
        out_filename=output_prefix+'_'+bytes((int_charcode,)).decode()+'_'+out_descr+'.osm.bz2'

        bbox_as_str=','.join(map(str,bbox))
        stt=datetime.datetime.now()
        print('osmium','--bbox='+bbox_as_str,'>',out_filename)
        subprocess.check_call(['osmium','extract','--bbox',bbox_as_str,input_file,'--output',out_filename])

        dt=datetime.datetime.now()-stt
        print('extracted',round(os.path.getsize(out_filename)/1e6,1),'MB for this bzip2 split in',dt)
        yield out_filename

def multi_osm_to_obf_pgsql2osm(access:psycopg2.extensions.connection,stripes:typing.Iterator[bbox_t],
        output_prefix:str,osm_rel_id:int)->typing.Iterator[str] :

    m=pgsql2osm.ModuleSettings(
        bounds_rel_id=osm_rel_id,
        get_lonlat_binary='/home/user/src/osm2pgsql/build/get_lonlat',
        nodes_file='/mnt/dbp/maps/planet.bin.nodes',
        access=access)

    stripes=list(stripes) #collapse generator
    print('getting',len(stripes),'stripes')
    for bbox in stripes :
        stt=datetime.datetime.now()
        out_fn=run_pgsql2osm(m,bbox,output_prefix)
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

def statically_get_splits(bbox:bbox_t,target_area=2.0) ->typing.Iterator[bbox_t] :
    """ Without regarding to-extract area, just split bbox into rectangles
    of size target_area square degrees.
    """
    # step bbox[0] and bbox[2] by 1, and bbox[1] bbox[3] by target_area.
    x_start=bbox[0]
    x_curr=x_start
    y_start=bbox[1]
    y_curr=y_start
    def clip_to(a,b,c,d) :
        rslt=[a,b,c,d]
        if rslt[0]<bbox[0] :
            rslt[0]=bbox[0]
        if rslt[1]<bbox[1] :
            rslt[1]=bbox[1]
        if rslt[2]>bbox[2] :
            rslt[2]=bbox[2]
        if rslt[3]>bbox[3] :
            rslt[3]=bbox[3]
        return rslt

    while x_start<bbox[2] :
        while y_start<bbox[3] :
            x_curr+=1.0
            y_curr+=target_area
            yield clip_to(x_start,y_start,x_curr,y_curr)
            #move down : y++
            x_curr=x_start
            y_start=y_curr
        #move right: x++
        x_start+=1.0
        x_curr=x_start
        y_start=bbox[1]
        y_curr=y_start


def osmium_get_extent(filename:str)->(float,float,float,float) :
    rslt=subprocess.check_output(['osmium','fileinfo','--json',filename]).decode('utf-8')
    accum_bbox=[180,90,-180,-90]
    for a,b,c,d in json.loads(rslt)['header']['boxes'] :
        if a<accum_bbox[0] :
            accum_bbox[0]=a
        if b<accum_bbox[1] :
            accum_bbox[1]=b
        if c>accum_bbox[2] :
            accum_bbox[2]=c
        if d>accum_bbox[3] :
            accum_bbox[3]=d
    return accum_bbox

def run_java_mapcreator(*args:str,**kwargs)->datetime.timedelta :
    start_t=datetime.datetime.now()
    env=[]
    for k,v in kwargs.items() :
        env.append(f'{k}={v}')
    if len(env)!=0 :
        env.insert(0,'env')
    try :
        a=subprocess.Popen(
            [*env,'bash','run_mapcreator.sh',*args],
            stdout=subprocess.PIPE,stderr=subprocess.PIPE
        )
        a.wait()
    except subprocess.CalledProcessError as err:
        print(a.stdout.read())
        print(a.stderr.read())
        raise err
    return datetime.datetime.now()-start_t


def assemble_splits_to_obf(input_splits:typing.Iterator[str],output_prefix:str) :
    input_splits=list(input_splits) #in case of a generator, collapse it (for reading multiple times)
    max_ram='10G'
    max_ram_int=round(sum(map(os.path.getsize,input_splits))*10/len(input_splits)*1e-9)
    max_ram=str(min(max_ram_int,1))+'G' #?
    print('max_ram',max_ram)
    work_dir='/'.join(output_prefix.split('/')[:-1])
    obf_splits=[]
    for osm_split in input_splits :
        #yes the java does that .split('.')[0].capitalize() internally...
        deduced_out_obf=osm_split.split('/')[-1].split('.')[0]
        deduced_out_obf=work_dir+'/'+deduced_out_obf.capitalize()+'.obf'
        os.chdir(work_dir)
        print('generating obf',osm_split,'->',deduced_out_obf)
        print('done in',run_java_mapcreator('generate-obf','--ram-process',osm_split,MAX_RAM=max_ram))
        obf_splits.append(deduced_out_obf)

    out_obf=output_prefix+'.obf'
    os.chdir(work_dir)
    print('merging obfs',obf_splits,'->',out_obf)
    print('done in',run_java_mapcreator('merge-index','--address','--poi',out_obf,*obf_splits,MAX_RAM=max_ram))


"""
Usage:
    This script manages different programs together to produce a .obf file at the end.
    Input data is read from the database and converted to .osm.bz2 with pgsql2osm.py
    in stripes.
    Those stripes are then individually converted to .obf with the RAM-hungry
        process: do not paralellize unless you have >64GB RAM to spare
    And finally, all stripes are merged into one big .obf

"""

if __name__=='__main__' :
    dbaccess,osm_rel_id,output_prefix=sys.argv[1:]

    if True : #TEMP
        filename=osm_rel_id
        out_splits_osm=list(multi_osm_to_obf_osmium(
            statically_get_splits(osmium_get_extent(filename)),filename,output_prefix
        ))
    else :
        osm_rel_id=int(osm_rel_id)
        a=psycopg2.connect(dbaccess)
        stripes=get_stripes_by_area(calculate_areas(a.cursor()))
        out_splits_osm=list(multi_osm_to_obf_pgsql2osm(a,stripes,output_prefix,osm_rel_id))

    assemble_splits_to_obf(out_splits_osm,output_prefix)

    [os.remove(i) for i in out_splits_osm]
