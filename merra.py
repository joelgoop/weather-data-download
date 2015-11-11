import logging
import os
import datetime
from urllib import urlencode
from itertools import chain,product
import glob
import utils
import itertools as it

logger = logging.getLogger(__name__)

WIND_PRESET = {
    'variables': [d+h+'m' for d,h in product(['v','u'],['2','10','50'])]+['disph'],
    'dataset': 'tavg1_2d_slv_Nx',
    'shortname': 'MAT1NXSLV'
}

SOLAR_PRESET = {
    'variables': ['ts','albedo','albvisdf','albvisdr','swtdn','swgdn'],
    'dataset': 'tavg1_2d_rad_Nx',
    'shortname': 'MAT1NXRAD'
}

def create_url(date,dataset,shortname,variables,dataformat=('SERGLw','hdf'),bbox=(30,-15,75,42.5),dataset_version='5.2.0'):
    """
    Create URL string to download MERRA data from a given dataset for specific date.

    Args:
        date (datetime.date): date for which to get data
        dataset (str): name of MERRA dataset (e.g. tavg1_2d_slv_Nx)
        shortname (str): short name of MERRA dataset (e.g. MAT1NXSLV)
        variables (list): names for variables in MERRA
        bbox (4-tuple of floats): bounding box in lat/long as tuple (lower lat, lower 
            long, upper lat, upper long) default is main Europe

    Returns:
        A tuple with URL string and label.
    """
    fmt_code,ext = dataformat
    bbox_str = '{},{},{},{}'.format(*bbox)
    logger.debug("Create URL to get {} from {} ({}) for {} within ({}).".format(','.join(variables),dataset,shortname,date,bbox_str))

    # MERRA200 from Jan 1 1992
    merra200_breakpoint = datetime.date(1992,1,1)
    # MERRA300 from Jan 1 2001
    merra300_breakpoint = datetime.date(2001,1,1)
    # tavg1_2d_rad_Nx update date: Jan 1 2010
    rad_update_date = datetime.date(2010,1,1)

    # Set MERRA version based on date
    if date < merra200_breakpoint:
        merra_version = 100
    elif date < merra300_breakpoint:
        merra_version = 200
    else:
        merra_version = 300
    logger.debug('For {}, the MERRA version is {}'.format(date,merra_version))

    # When data is updated by NASA, the new versions are called 101, 201, 301
    # For the 'tavg1_2d_rad_Nx' dataset all files before Jan 1st 2010 have been updated
    if dataset == 'tavg1_2d_rad_Nx' and date < rad_update_date:
        merra_version += 1
        logger.debug('{} before {} has been updated, changing version to {}.'.format(dataset,rad_update_date,merra_version))

    merra_args = {
        'BBOX': bbox_str,
        'FILENAME': r'/data/s4pa/MERRA/{shortname}.{dataset_version}/{date.year}/{date.month:02d}/MERRA{merra_version}.prod.assim.{dataset}.{date.year}{date.month:02d}{date.day:02d}.hdf' \
                        .format(date=date,merra_version=merra_version,shortname=shortname,dataset=dataset,dataset_version=dataset_version),
        'FORMAT': fmt_code,
        'LABEL': 'MERRA{merra_version}.prod.assim.{dataset}.{date.year}{date.month:02d}{date.day:02d}.SUB.{ext}' \
                        .format(date=date,merra_version=merra_version,dataset=dataset,ext=ext),
        'VARIABLES': ','.join(variables),
        'VERSION': '1.02',
        'SHORTNAME': shortname,
        'SERVICE': 'SUBSET_LATS4D',
    }

    return (r"http://goldsmr2.sci.gsfc.nasa.gov/daac-bin/OTF/HTTP_services.cgi?{}".format(urlencode(merra_args)),merra_args['LABEL'])


def download(years,dest,datatype,filefmt,**kwargs):
    """
    Create date range and downloads file for each date.

    Args:
        years (iterable): years for which to download data
        dest (str): path to destination directory
    """
    options = {}

    if filefmt=='nc':
        options.update({'dataformat': ('TmV0Q0RGLw','nc')})
    elif filefmt=='hdf':
        options.update({'dataformat': ('SERGLw','hdf')})
    else:
        raise(ArgumentError('Unexpected file format: {}'.format(filefmt)))

    if datatype=='wind':
        options.update(WIND_PRESET)
    elif datatype=='solar':
        options.update(SOLAR_PRESET)
    else:
        raise(ArgumentError('Unexpected data type: {}'.format(datatype)))

    dates = chain(*[utils.daterange(start_date=datetime.date(year,1,1),end_date=datetime.date(year+1,1,1)) for year in years])
    for date in dates:
        download_date(date,dest,**options)
        logger.info('Downloaded for {}.'.format(date))


def download_date(date,dest,**kwargs):
    """
    Download MERRA data for specific date into target directory.

    Args:
        date (datetime.date): date for which to download data
        dest (str): path to destination directory
        kwargs: keyword arguments to be sent to create_url
    """
    url,label = create_url(date,**kwargs)
    target_file = os.path.join(dest,label)

    logger.debug('Attempting to download. URL:\n{}\nTarget file: {}'.format(url,target_file))
    utils.dlfile(url,target_file)


def clean(source,dest,ext='hdf',datatype=None,**kwargs):
    """
    Concatenate data from separate files into one file for each year.

    Args:
        source (str): path to data files
        dest (str): path to save output
        ext (str): extension for data files
        datatype: either 'wind','solar', or None
    """
    import h5py
    import pyhdf.SD as h4
    from pyhdf.error import HDF4Error
    import re

    files = sorted(glob.glob(os.path.join(source,'*.'+ext)))

    # Set dataset name to match based on datatype if set
    if datatype == 'wind':
        ds_match = re.escape(WIND_PRESET['dataset'])
    elif datatype == 'solar':
        ds_match = re.escape(SOLAR_PRESET['dataset'])
    else:
        ds_match = '[^\.]+'

    # Regex to match year in names like MERRA300.prod.assim.tavg1_2d_slv_Nx.20010101.SUB.hdf
    regex_y = re.compile(r'MERRA[0-9]{3}\.prod\.assim.(?P<dataset>'+ds_match+r')\.(?P<year>\d{4})\d{4}\..*\.'+ext)
    # Group files by year to concatenate data for each year into one output file
    files_years = group_by_tuple(files,regex_y,keys=['dataset','year'])

    # Regex to match date in names like MERRA300.prod.assim.tavg1_2d_slv_Nx.20010101.SUB.hdf
    regex_d = re.compile(r'MERRA[0-9]{3}\.prod\.assim.'+ds_match+r'\.(?P<date>\d{8})\..*\.'+ext)

    for (dataset,year),files in files_years:
        start_time = datetime.datetime(int(year),1,1,0,0)
        end_time = datetime.datetime(int(year)+1,1,1,0,0)
        hours = list(utils.hourrange(start_time,end_time))
        numhours = len(hours)
        logger.debug('Listed {} hours during year {}.'.format(numhours,year))
        logger.info('Parsing {} data for year {}.'.format(dataset,year))
        with h5py.File(os.path.join(dest,'{}.{}.{}'.format(dataset,year,ext))) as outfile:
            for path in files:
                # Extract name of file only and search for date
                fname = os.path.basename(path)
                m = regex_d.search(fname)

                if m is not None:
                    # Calculate index of starting hour from beginning of year
                    cur_time = datetime.datetime.strptime(m.group('date'), '%Y%m%d')
                    start_hour = int((cur_time - start_time).total_seconds()/3600)
                    logger.debug('Reading file for {}: {}'.format(cur_time,fname))
                    try:
                        h4_file = h4.SD(path)
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
                            if v not in outfile:
                                logger.debug('Creating dataset {} with size ({},{},{}).'.format(v,numhours,numlats,numlongs))
                                outfile.create_dataset(v,(numhours,numlats,numlongs))
                            
                            data = h4_file.select(v).get()
                            logger.debug('Assign \'{}\', time steps {}:{}, data with shape {}'.format(v,start_hour,start_hour+len(ts),str(data.shape)))
                            outfile[v][start_hour:start_hour+len(ts),:,:] = data


                    except HDF4Error as e:
                        logger.exception(e)
                else:
                    logger.warning('Filename could not be parsed: '+fname)

def group_by_tuple(iterator,regex,keys):
    """
    Group an iterator of strings by keys for groups in a regex.

    Args:
        iter: iterator with strings to group
        regex: compiled regular expression object
        keys: keys for matching groups

    Returns:
        grouped iterator
    """
    def keyfunc(s):
        m = regex.search(s)
        if m is not None:
            return tuple(m.group(k) for k in keys)
        else:
            logger.warning("Keys '{}' not found in {}.".format(','.join(keys),s))
            return None
    return it.groupby(iterator,keyfunc)