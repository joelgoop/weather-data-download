import logging
import os
import datetime
from urllib import urlencode
from itertools import chain,product
import glob
import utils
import itertools as it

logger = logging.getLogger('weather-data-download')

PRESETS = {
    # MERRA settings
    'merra': {
        'datatypes': {
            'wind': {
                'variables': [d+h+'m' for d,h in product(['v','u'],['2','10','50'])]+['disph'],
                'dataset': 'tavg1_2d_slv_Nx',
                'shortname': 'MAT1NXSLV',
                'data_version': '5.2.0'
            },
            'solar': {
                'variables': ['ts','albedo','albvisdf','albvisdr','swtdn','swgdn'],
                'dataset': 'tavg1_2d_rad_Nx',
                'shortname': 'MAT1NXRAD',
                'data_version': '5.2.0'
            }
        },
        'fileformats': {
            'hdf': ('SERGLw','hdf'),
            'nc': ('TmV0Q0RGLw','nc'),
            'default': ('SERGLw','hdf')
        },
        # Increase version by 100 at each breakpoint
        'version_breakpoints': [
            datetime.date(1993,1,1), # MERRA200 after 1/1 1993
            datetime.date(2001,1,1) # MERRA300 after 1/1 2001
        ],
        'service': 'SUBSET_LATS4D',
        'version': '1.02',
        'filename': r'/data/s4pa/MERRA/{shortname}.{data_version}/{date.year}/{date.month:02d}/MERRA{merra_version}.prod.assim.{dataset}.{date.year}{date.month:02d}{date.day:02d}.{ext}',
        'label': 'MERRA{merra_version}.prod.assim.{dataset}.{date.year}{date.month:02d}{date.day:02d}.SUB.{ext}',
        'base_url': r'http://goldsmr2.sci.gsfc.nasa.gov/daac-bin/OTF/HTTP_services.cgi?'
    },

    # MERRA-2 settings
    'merra2': {
        'datatypes': {
            'wind': {
                'variables': [d+h+'m' for d,h in product(['v','u'],['2','10','50'])]+['disph'],
                'dataset': 'tavg1_2d_slv_Nx',
                'shortname': 'M2T1NXSLV',
                'data_version': '5.12.4',
            },
            'solar': {
                'variables': ['ts','albedo','albvisdf','albvisdr','swtdn','swgdn'],
                'dataset': 'tavg1_2d_rad_Nx',
                'shortname': 'M2T1NXRAD',
                'data_version': '5.12.4',
            }
        },
        'fileformats': {
            'nc4': ('bmM0Lw','nc4'),
            'default': ('bmM0Lw','nc4')
        },
        'version_breakpoints': [
            datetime.date(1992,1,1), # MERRA200 after 1/1 1992
            datetime.date(2001,1,1), # MERRA300 after 1/1 2001
            datetime.date(2011,1,1) # MERRA400 after 1/1 2011
        ],
        'service': 'SUBSET_MERRA2',
        'version': '1.02',
        'label': 'svc_MERRA2_{merra_version}.{dataset}.{date.year}{date.month:02d}{date.day:02d}.{ext}',
        'filename': r'/data/s4pa/MERRA2/{shortname}.{data_version}/{date.year}/{date.month:02d}/MERRA2_{merra_version}.{dataset}.{date.year}{date.month:02d}{date.day:02d}.{ext}',
        'base_url': r'http://goldsmr4.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi?'
    }
}

BBOX_PRESETS = {
    'europe': (30,-15,75,42.5)
}

def create_url(date,datatype,settings,filefmt,revision,bbox='europe'):
    """
    Create URL string to download MERRA data from a given dataset for specific date.

    Args:
        date (datetime.date): date for which to get data
        datatype (str): which set of variables and settings to use 'wind' or 'solar'
        settings (dict): dictionary of settings
        filefmt (str): format key
        revision (int): version above main version number, i.e., 
            0 gives 100, 200 or 300, while 1 gives 101, 201 or 301
        bbox: key to presets or 4-tuple with coordinates

    Returns:
        A tuple with URL string and label.
    """
    if bbox in BBOX_PRESETS:
        bbox_tup = BBOX_PRESETS[bbox]
    else:
        bbox_tup = bbox
    bbox_str = '{},{},{},{}'.format(*bbox_tup)

    fmt_code,ext = settings['fileformats'][filefmt]

    data_info = {
        'ext': ext,
        'date': date
    }
    data_info.update(settings['datatypes'][datatype])

    logger.debug("Create URL to get {} from {dataset} ({shortname}) for {date} within ({bbox_str}).".format(
        ','.join(data_info['variables']),bbox_str=bbox_str,**data_info))

    merra_version = 100
    for b in settings['version_breakpoints']:
        if date >= b:
            merra_version += 100
    merra_version += revision
    data_info['merra_version'] = merra_version

    logger.debug('For {}, the MERRA version is {} (revision {}).'.format(date,merra_version,revision))

    merra_args = {
        'BBOX': bbox_str,
        'FILENAME': settings['filename'].format(**data_info),
        'FORMAT': fmt_code,
        'LABEL':  settings['label'].format(**data_info),
        'VARIABLES': ','.join(data_info['variables']),
        'VERSION': settings['version'],
        'SHORTNAME': data_info['shortname'],
        'SERVICE': settings['service'],
    }

    return (settings['base_url']+urlencode(merra_args), merra_args['LABEL'])


def download(years,datasource,dest,skip_existing,datatype,filefmt):
    """
    Create date range and downloads file for each date.

    Args:
        years (iterable): years for which to download data
        datasource (str): source ('merra' or 'merra2')
        dest (str): path to destination directory
        skip_existing (bool): skip download if target exists
        datatype (str): choose 'wind' or 'solar' data for presets
        filefmt (str): file format to download
    """
    try:
        options = PRESETS[datasource]
    except KeyError as e:
        raise ArgumentError("Unknown datasource '{}'".format(datasource))

    if datatype not in options['datatypes']:
        raise ArgumentError("Unknown datatype '{}' for source '{}'".format(datatype,datasource))
    if filefmt not in options['fileformats']:
        raise ArgumentError("Unknown file format '{}' for source '{}'".format(filefmt,datasource))
    

    dates = chain(*[utils.daterange(start_date=datetime.date(year,1,1),
                                    end_date=datetime.date(year+1,1,1)) for year in years])
    pool = utils.ThreadPool(4)

    def dl_task(date):
        download_date(date,dest,skip_existing,settings=options,
            datatype=datatype,filefmt=filefmt)

    for date in dates:
        pool.add_task(dl_task,date)

    pool.wait_completion()


def download_date(date,dest,skip_existing=False,**kwargs):
    """
    Download MERRA data for specific date into target directory.

    Args:
        date (datetime.date): date for which to download data
        dest (str): path to destination directory
        skip_existing (bool): whether to skip if file exists
        kwargs: keyword arguments to be sent to create_url
    """
    def try_download_revision(revision):
        url,label = create_url(date=date,revision=revision,**kwargs)
        target_file = os.path.join(dest,label)
        logger.debug('Attempting to download. URL:\n{}\nTarget file: {}'.format(url,target_file))
        if not (os.path.isfile(target_file) and skip_existing):
            utils.dlfile(url,target_file)
            logger.info('Downloaded for {}.'.format(date))
        else:
            logger.info('Target for {} exists. Skipping.'.format(date))

    utils.retry_with_args(
        try_download_revision,[0,1,2],
        exc=utils.URLNotFoundException,
        delay=1)


def clean_merra(source,dest,ext='hdf',out_ext='hdf',datatype=None,**kwargs):
    """
    Concatenate data from separate files into one file for each year.

    Args:
        source (str): path to data files
        dest (str): path to save output
        ext (str): extension for data files
        datatype: either 'wind','solar', or None
    """
    logger.debug('Applying MERRA2 data cleaning function.')

    import h5py
    import pyhdf.SD as h4
    from pyhdf.error import HDF4Error
    import re

    files = sorted(glob.glob(os.path.join(source,'*.'+ext)))

    # Set dataset name to match based on datatype if set
    if datatype not in PRESETS['merra']['datatypes']:
        raise ArgumentError("Unknown datatype '{}' for datasource 'merra'".format(datatype))
    ds_match = re.escape(PRESETS['merra']['datatypes'][datatype]['dataset'])

    # Regex to match year in names like MERRA300.prod.assim.tavg1_2d_slv_Nx.20010101.SUB.hdf
    regex_y = re.compile(r'MERRA[0-9]{3}\.prod\.assim.(?P<dataset>'+ds_match+r')\.(?P<year>\d{4})\d{4}\..*\.'+ext)
    # Group files by year to concatenate data for each year into one output file
    files_years = utils.group_by_tuple(files,regex_y,keys=['dataset','year'])

    # Regex to match date in names like MERRA300.prod.assim.tavg1_2d_slv_Nx.20010101.SUB.hdf
    regex_d = re.compile(r'MERRA[0-9]{3}\.prod\.assim.'+ds_match+r'\.(?P<date>\d{8})\..*\.'+ext)

    for (dataset,year),files in files_years:
        start_time = datetime.datetime(int(year),1,1,0,0)
        end_time = datetime.datetime(int(year)+1,1,1,0,0)
        hours = list(utils.hourrange(start_time,end_time))
        numhours = len(hours)
        logger.debug('Listed {} hours during year {}.'.format(numhours,year))
        logger.info('Parsing {} data for year {}.'.format(dataset,year))
        with h5py.File(os.path.join(dest,'{}.{}.{}'.format(dataset,year,out_ext))) as outfile:
            for path in files:
                logger.debug('Path is {} (type: {}).'.format(path,type(path)))
                # Extract name of file only and search for date
                fname = os.path.basename(path)
                m = regex_d.search(fname)

                if m is not None:
                    # Calculate index of starting hour from beginning of year
                    cur_time = datetime.datetime.strptime(m.group('date'), '%Y%m%d')
                    start_hour = int((cur_time - start_time).total_seconds()/3600)
                    logger.debug('Reading file for {}: {}'.format(cur_time,fname))
                    try:
                        h4_file = h4.SD(path.encode('ascii'))
                        ts = h4_file.select('time').get()
                        lats = h4_file.select('latitude').get()
                        longs = h4_file.select('longitude').get()
                        numlats,numlongs = len(lats),len(longs)

                        # Create latitude and longitude from input file if not found in output
                        if any(k not in outfile for k in ['latitude','longitude','time']):
                            logger.debug('Creating lat/long and time sets with lengths {}, {} and {}.'.format(numlats,numlongs,numhours))
                            outfile['latitude'] = lats
                            outfile['longitude'] = longs
                            outfile.create_dataset('time',data=map(utils.to_timestamp,hours),dtype='int64')

                        for v in (ds for  ds in h4_file.datasets() if ds not in  ['latitude','longitude','time']):
                            if v.lower() not in outfile:
                                logger.debug('Creating dataset {} with size ({},{},{}).'.format(v.lower(),numhours,numlats,numlongs))
                                outfile.create_dataset(v.lower(),(numhours,numlats,numlongs))
                            
                            data = h4_file.select(v).get()
                            logger.debug('Assign \'{}\', time steps {}:{}, data with shape {}'.format(v,start_hour,start_hour+len(ts),str(data.shape)))
                            outfile[v.lower()][start_hour:start_hour+len(ts),:,:] = data


                    except HDF4Error as e:
                        logger.exception(e)
                else:
                    logger.warning('Filename could not be parsed: '+fname)


def clean_merra2(source,dest,ext='nc4',out_ext='hdf',datatype=None,**kwargs):
    """
    Concatenate MERRA2 data from separate files into one file for each year.

    Args:
        source (str): path to data files
        dest (str): path to save output
        ext (str): extension for data files
        datatype: either 'wind','solar', or None
    """
    logger.debug('Applying MERRA2 data cleaning function.')
    import h5py
    import re

    files = sorted(glob.glob(os.path.join(source,'*.'+ext)))

    # Set dataset name to match based on datatype if set
    if datatype not in PRESETS['merra2']['datatypes']:
        raise ArgumentError("Unknown datatype '{}' for datasource 'merra'".format(datatype))
    ds_match = re.escape(PRESETS['merra2']['datatypes'][datatype]['dataset'])

    # Regex to match year in names like svc_MERRA2_100.tavg1_2d_slv_Nx.19800101.nc4
    regex_y = re.compile(r'svc_MERRA2_[0-9]{3}\.(?P<dataset>'+ds_match+r')\.(?P<year>\d{4})\d{4}\.'+ext)

    # Group files by year to concatenate data for each year into one output file
    files_years = utils.group_by_tuple(files,regex_y,keys=['dataset','year'])

    # Regex to match date in names like svc_MERRA2_100.tavg1_2d_slv_Nx.19800101.nc4
    regex_d = re.compile(r'svc_MERRA2_[0-9]{3}\.(?P<dataset>'+ds_match+r')\.(?P<date>\d{8})\.'+ext)

    for (dataset,year),files in files_years:
        start_time = datetime.datetime(int(year),1,1,0,0)
        end_time = datetime.datetime(int(year)+1,1,1,0,0)
        hours = list(utils.hourrange(start_time,end_time))
        numhours = len(hours)
        logger.debug('Listed {} hours during year {}.'.format(numhours,year))
        logger.info('Parsing {} data for year {}.'.format(dataset,year))
        with h5py.File(os.path.join(dest,'{}.{}.{}'.format(dataset,year,out_ext))) as outfile:
            for path in files:
                logger.debug('Path is {} (type: {}).'.format(path,type(path)))
                # Extract name of file only and search for date
                fname = os.path.basename(path)
                m = regex_d.search(fname)

                if m is not None:
                    # Calculate index of starting hour from beginning of year
                    cur_time = datetime.datetime.strptime(m.group('date'), '%Y%m%d')
                    start_hour = int((cur_time - start_time).total_seconds()/3600)
                    logger.debug('Reading file for {}: {}'.format(cur_time,fname))
                    with h5py.File(path.encode('ascii')) as h5_file:
                        ts = h5_file['time'][:]
                        lats = h5_file['lat'][:]
                        longs = h5_file['lon'][:]
                        numlats,numlongs = len(lats),len(longs)

                        # Create latitude and longitude from input file if not found in output
                        if any(k not in outfile for k in ['latitude','longitude','time']):
                            logger.debug('Creating lat/long and time sets with lengths {}, {} and {}.'.format(numlats,numlongs,numhours))
                            outfile['latitude'] = lats
                            outfile['longitude'] = longs
                            outfile.create_dataset('time',data=map(utils.to_timestamp,hours),dtype='int64')

                        for v in (ds for  ds in h5_file if ds not in  ['lat','lon','time']):
                            if v.lower() not in outfile:
                                logger.debug('Creating dataset {} with size ({},{},{}).'.format(v.lower(),numhours,numlats,numlongs))
                                outfile.create_dataset(v.lower(),(numhours,numlats,numlongs))
                            
                            data = h5_file[v][:]
                            logger.debug('Assign \'{}\', time steps {}:{}, data with shape {}'.format(v.lower(),start_hour,start_hour+len(ts),str(data.shape)))
                            outfile[v.lower()][start_hour:start_hour+len(ts),:,:] = data
                else:
                    logger.warning('Filename could not be parsed: '+fname)