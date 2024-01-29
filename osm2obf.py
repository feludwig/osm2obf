#!/usr/bin/python3

import sys
import subprocess
import json
import bz2
import os
import datetime
import typing
import argparse

"""
TODO
    max_ram approach: retry without --ram-process and only then give up
"""

#relative to self location
for i in ['../pgsql2osm','pgsql2osm'] :
    sys.path.append(i)
    sys.path.append(os.path.dirname(__file__)+'/'+i)
try :
    import pgsql2osm
    import psycopg2
    available_pgsgl2osm=True
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

def run_pgsql2osm(m,bbox:bbox_t,outfile_pfx:str) :
    """ m:pgsql2osm.ModuleSettings but pgsql2osm may not be defined at this point
    """
    st_x_s=str(bbox[0]).replace('.','_')
    en_x_s=str(bbox[2]).replace('.','_')
    outfile=f'{outfile_pfx}_{st_x_s}-{en_x_s}'.replace('.','_')
    outfile+='_split.osm.bz2'
    bbox_as_str=','.join(map(str,bbox))
    print('pgsql2osm','--bbox='+bbox_as_str,'| bzip2 >',os.path.basename(outfile),'...')
    sys.stdout.flush()

    stt=datetime.datetime.now()
    with bz2.open(outfile,'wb') as f:
        m.bounds_box=bbox_as_str
        m.out_file=f
        m.main()

    dt=datetime.datetime.now()-stt
    print(round(os.path.getsize(outfile)/1e6,1),'MB for this bzip2 split in',dt)
    sys.stdout.flush()
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
        out_filename=output_prefix+'_'+bytes((int_charcode,)).decode()+'_'+out_descr+'_split.osm.bz2'

        bbox_as_str=','.join(map(str,bbox))
        stt=datetime.datetime.now()
        print('osmium','--bbox='+bbox_as_str,'>',out_filename)
        subprocess.check_call(['osmium','extract','--bbox',bbox_as_str,input_file,'--output',out_filename])

        dt=datetime.datetime.now()-stt
        print('extracted',round(os.path.getsize(out_filename)/1e6,1),'MB for this bzip2 split in',dt)
        yield out_filename

def multi_osm_to_obf_pgsql2osm(access:psycopg2.extensions.connection,stripes:typing.Iterator[bbox_t],
        output_prefix:str,args:argparse.Namespace)->typing.Iterator[str] :

    stripes=list(stripes) #collapse generator
    print('getting',len(stripes),'stripes')

    m=pgsql2osm.settings.ModuleSettings(
        #copy these keys, required for pgsql2osm
        **{k:getattr(args,k) for k in ('bounds_rel_id','bounds_iso','bounds_geojson',
            'get_lonlat_binary','nodes_file')},
        has_suggested_out_filename=True, #do not print suggestion filename
        access=access)
    for bbox in stripes :
        stt=datetime.datetime.now()
        out_fn=run_pgsql2osm(m,bbox,output_prefix)
        dt=datetime.datetime.now()-stt
        print('extracted',out_fn,'in',dt)
        yield out_fn #after printing

def calculate_areas(c:psycopg2.extensions.cursor,args:argparse.Namespace)->typing.Dict[int,float] :
    print('calculating area split...')
    sys.stdout.flush()
    from_rel_id=False
    if args.bounds_geojson!=None :
        with open(args.bounds_geojson,'r') as f :
            geojson=f.read().strip()
        way=f"(ST_Transform(ST_GeomFromGeoJSON('{geojson}'::jsonb),3857))"
    elif args.bounds_rel_id!=None :
        osm_rel_id=args.bounds_rel_id
        from_rel_id=True
    elif args.bounds_iso!=None :
        c_name,osm_rel_id=pgsql2osm.dbutils.regions_lookup(args.bounds_iso)
        osm_rel_id=int(osm_rel_id)
        from_rel_id=True

    if from_rel_id :
        #relation ids are stored negative
        way=f'(SELECT ST_Transform(relbound.way,3857) FROM planet_osm_polygon AS relbound WHERE osm_id={-osm_rel_id})'

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
                    ST_Intersection({way},a_3857),4326)),
            (x*100)::int AS x
        FROM (SELECT ST_Transform(
                ST_MakeEnvelope(x,-89.999,x+{split_size},89.999,4326),3857) AS a_3857,x
            FROM generate_series(-180.0,180.0,{split_size}) AS x
        ) AS foo
        WHERE ST_Intersects(a_3857,{way});"""
    c.execute(q)
    areas={}
    for (area,x) in c.fetchall() :
        areas[x]=area
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

class OsmAndRunner :
    def __init__(self, osmand_abs_dir:str, output_prefix:str, verbose=False) :
        self.output_prefix=output_prefix
        work_dir=os.path.dirname(self.output_prefix)
        self.config={'WORK_DIR':work_dir,'ABS_DIR':osmand_abs_dir}
        self.verbose=verbose

    def run_java_mapcreator(self,*args:str)->datetime.timedelta :
        """ Drive the java OsmAndMapCreator program, with RAM limits set,
        and in the correct working directory
        """

        sys.stdout.flush()
        start_t=datetime.datetime.now()
        config={'maxram':None,'minram':None,'absdir':None,'workdir':None}
        for k,v in self.config.items() :
            if k=='MAX_RAM' :
                config['maxram']=v
                if config['minram']==None :
                    #default: deduce automatically
                    config['minram']='1'+v[-1] # v=35G -> 1G, v=125M -> 1M
            elif k=='MIN_RAM' :
                config['minram']=v
            elif k=='ABS_DIR' :
                config['absdir']=v
                assert v[0]=='/', "Need to provide absolute path (eg /home/user/OsmAndMapCreator/, not just OsmAndMapCreator/)"
                if len(config['absdir'])>0 and config['absdir'][-1]=='/' :
                    #remove trailing slash
                    config['absdir']=config['absdir'][:-1]
            elif k=='WORK_DIR' :
                config['workdir']=v
                if len(config['workdir'])>0 and config['workdir'][-1]=='/' :
                    #remove trailing slash
                    config['workdir']=config['workdir'][:-1]

        #check required keys
        for k in ('workdir','absdir','maxram') :
            k_show=k.upper().replace('DIR','_DIR').replace('RAM','_RAM')
            assert config[k] is not None,f'Error: config value {k_show} not provided'
        return_dir=os.getcwd()
        os.chdir(config['workdir'] if len(config['workdir'])>0 else '.')

        cmd=['env',f'JAVA_OPTS=-Xms{config["minram"]} -Xmx{config["maxram"]}']
        cmd.extend(['bash',config['absdir']+'/utilities.sh',*args])
        #closed stdin is required
        a=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout,stderr=a.communicate(None,timeout=48*3600) #wait 48h, crash if not finished by then
        os.chdir(return_dir)
        if self.verbose :
            print('\n'*5)
        if a.returncode!=0 :
            print(stdout.decode())
            print(stderr.decode())
            if stdout.decode().find('java.lang.OutOfMemoryError')>=0 :
                print('java ran out of memory')
                print('TODO: retry without --ram-process? then retry with higher MAX_RAM')
            exit(a.returncode)
        return datetime.datetime.now()-start_t

    def check_obf_splits(self,ls:list[str])->typing.Iterator[list[str]] :
        """ For a list of input filenames, check their sizes and if they
        are too big in total (hard limit 2GB for obf format), yield
        the split up list so that seach split is <2GB.
        """

        running_total=0
        running_list=[]
        printed_warn=False
        for i in ls :
            sz=os.path.getsize(i)
            running_total+=sz
            #print(running_total,'\t',sz,i)
            if running_total>2.1e9 :
                if not printed_warn :
                    printed_warn=True
                    msg=f'WARNING: Total size of {round(running_total*1e-9,2)}G '
                    msg+='is too big: obf format only supports max 2GB per file. '
                    msg+='Will create mutiple <2GB obfs\n'
                    msg+='RECOMMENDED: make multiple smaller obfs of provinces, '
                    msg+='instead of getting north-south stripes like this script will do now...'
                    print(msg)
                yield running_list #without current i
                running_list=[]
            running_list.append(i)
        yield running_list

    def set_max_ram(self,input_splits:typing.List[str],sum_mode=False) :
        if sum_mode :
            max_ram_int=round(sum(map(os.path.getsize,input_splits))*4*1e-9)
        else :
            max_ram_int=round(max(map(os.path.getsize,input_splits))*23*1e-9)

        max_ram=str(max(max_ram_int,1))+'G' #?
        print('max_ram',max_ram)
        self.config['MAX_RAM']=max_ram

    def convert_splits_to_obf(self,input_splits:typing.Iterator[str],
            skip_existing=False)->typing.Iterator[typing.List[str]] :
        """ Returns obf_splitss as a list of lists of obf filenames.
        This is to ensure the obf fileformat limit of 2GB is not reached (obf unsupported).
        Input: a list of .osm.bz2 or similar formats (osm,osm.pbf,osm.gz) filenames, each
        will be individually converted to obf.
        """
        input_splits=list(input_splits) #in case of a generator, collapse it (for reading multiple times)
        self.set_max_ram(input_splits)
        obf_splits=[]
        for osm_split in input_splits :
            #yes the java does that .split('.')[0].capitalize() internally...
            deduced_out_obf=osm_split.split('/')[-1].split('.')[0]
            deduced_out_obf=self.config['WORK_DIR']+'/'+deduced_out_obf.capitalize()+'.obf'
            if skip_existing and os.path.exists(deduced_out_obf) :
                continue
            print('generating',self.bn(osm_split),'->',self.bn(deduced_out_obf),'...')
            t=self.run_java_mapcreator('generate-obf','--ram-process',osm_split)
            print('generated in',t)
            obf_splits.append(deduced_out_obf)
        yield from self.check_obf_splits(obf_splits)

    def bn(self,inp) :
        """Basename alias, also accepts iterables"""
        if isinstance(inp,typing.Iterable) and not isinstance(inp,str):
            return repr([self.bn(i) for i in inp])
        return os.path.basename(inp)

    def assemble_splits_to_obf(self,obf_splitss:typing.Iterator[typing.List[str]]) :
        if len(obf_splitss)>1 :
            print('Outputting',len(obf_splitss),'<2GB obfs')
        for ix,obf_splits in enumerate(obf_splitss) :
            if len(obf_splitss)==1 :
                out_obf=self.output_prefix+'.obf'
            else :
                out_obf=self.output_prefix+f'_{ix+1}.obf'
            print('merging',self.bn(obf_splits),'->',self.bn(out_obf),'...')
            self.set_max_ram(obf_splits,sum_mode=True)
            t=self.run_java_mapcreator('merge-index','--address','--poi',out_obf,*obf_splits)
            if len(obf_splitss)==1 :
                print('merged in',t,';\t',self.bn(out_obf),os.path.getsize(out_obf),'bytes')
            else :
                print('merged',ix+1,'/',len(obf_splitss),'in',t,';\t',
                        self.bn(out_obf),os.path.getsize(out_obf),'bytes')


"""
Mechanism:
    This script manages different programs together to produce a .obf file at the end.
    Input data is read from the database and converted to .osm.bz2 with pgsql2osm.py
    or osmium, in stripes.
    Those stripes are then individually converted to .obf with the RAM-hungry
        process: do not paralellize unless you have >20GB RAM to spare
    And finally, all stripes are merged into one big .obf as long as the size sum
    is < 2GB (obf does not support bigger files).
"""

if __name__=='__main__' :
    parser=argparse.ArgumentParser(prog='osm2obf')
    subparsers = parser.add_subparsers(title='input modes',required=True,
        help="See '%(prog)s {mode} --help' for a mode's detailed options")

    s=subparsers.add_parser('postgres', help='''Read osm data from a osm2pgsql
        imported databe (MUST be with --slim), WARNING: --bbox unsupported for region choice''')
    s.set_defaults(mode='postgres')
    s.add_argument('get_lonlat_binary',
        help="Path to the get_lonlat binary")
    s.add_argument('nodes_file',
        help='Path to the nodes file created by osm2pgsql at import')
    s.add_argument('-d','--dsn',dest='postgres_dsn',
        default='dbname=gis port=5432',
        help="The connection string to pass to psycopg2, default '%(default)s'")
    bounds_g=s.add_mutually_exclusive_group(required=True)
    bounds_g.add_argument('-r','--osm-rel-id',dest='bounds_rel_id',
        default=None,type=int,
        help='Integer for the osm relation that should make the boundary')
    bounds_g.add_argument('-i','--iso',dest='bounds_iso',
        default=None,
        help='Country or region code for looking up in pgsql2osm/regions.csv, to determine boundary')
    bounds_g.add_argument('-g','--geojson',dest='bounds_geojson',
        default=None,
        help='Geojson file for determining the boundary')


    s=subparsers.add_parser('osmium', help='''Read osm data from a .osm
        (or any supported .osm.pbf, .osm.bz2 ...) with osmium (depends on osmium)''')
    s.set_defaults(mode='osmium')
    s.add_argument('-i','--input',dest='in_file',type=str,required=True,
        help="""Input filename, can be any format supported by osmium; for example .osm,
        .osm.pbf, .osm.bz2""")


    s=subparsers.add_parser('resume', help='''Resume a started extract, regardless of 'postgres' or
    'osmium' mode. All *_split.osm.bz2 files should be already created, this part will then put them
    together into .obf.''')
    s.set_defaults(mode='resume')


    parser.add_argument('-o','--output',dest='out_file',
        help="""Path where the output .obf should be written to. Writing to stdout is not supported.
        NOTE: the directory will be used a temporary working directory (to make all the *_split.osm.bz2 files, will need
        up to about 2x to 3x free space of original .osm.bz2 or .osm.pbf)""",
        required=True)
    parser.add_argument('--osmand',dest='osmand',default=os.path.dirname(__file__)+'/osmandmapcreator',
        help="""Directory where OsmAndMapCreator is zip-decompressed into. Default will try '%(default)s'.
        See README.md for how to download and decompress""")
    parser.add_argument('-k','--keep',dest='keep',action='store_true',default=False,
        help='Keep all intermediary *_split.osm.bz2 and *_split.obf files')


    args=parser.parse_args()

    args.out_prefix=os.path.abspath(args.out_file.replace('.obf',''))
    #make ABSdir
    args.osmand_abs_dir=os.path.abspath(os.path.realpath(args.osmand))

    #dbaccess,osmand_abs_dir,osm_rel_id,output_prefix=sys.argv[1:]
    ocr=OsmAndRunner(args.osmand_abs_dir,args.out_prefix)

    if args.mode=='postgres' :
        #pgsql2osm
        assert available_pgsgl2osm,'Required in pgsql2osm mode'
        a=psycopg2.connect(args.postgres_dsn)
        stripes=get_stripes_by_area(calculate_areas(a.cursor(),args))
        out_splits_osm=list(multi_osm_to_obf_pgsql2osm(a,stripes,args.out_prefix,args))
    elif args.mode=='osmium' :
        #osmium
        out_splits_osm=list(multi_osm_to_obf_osmium(
            statically_get_splits(osmium_get_extent(filename)),args.in_file,args.out_prefix
        ))
    elif args.mode=='resume' :
        #resume from existing splits files
        import glob
        out_splits_osm=list(sorted(glob.glob(args.out_prefix+'_*_split.osm.bz2')))
        #obf_splitss=list(ocr.check_obf_splits(sorted(glob.glob(output_prefix+'_*.obf'))))

    #need to collapse to list here, so that cleanup works properly
    obf_splitss=list(ocr.convert_splits_to_obf(out_splits_osm,skip_existing=args.mode=='resume'))
    ocr.assemble_splits_to_obf(obf_splitss)

    #cleanup
    if not args.keep :
        [os.remove(i) for i in out_splits_osm]
        [os.remove(i) for s in obf_splitss for i in s]
    regions_ocbf=os.path.dirname(args.out_prefix)+'/regions.ocbf'
    if os.path.exists(regions_ocbf) :
        os.remove(regions_ocbf)
