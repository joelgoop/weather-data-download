import logging
import os
import datetime
from urllib import urlencode
from itertools import chain,product
import utils

logger = logging.getLogger(__name__)

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
        options.update({
            'variables': [d+h+'m' for d,h in product(['v','u'],['2','10','50'])],
            'dataset': 'tavg1_2d_slv_Nx',
            'shortname': 'MAT1NXSLV'
        })
    elif datatype=='solar':
        options.update({
            'variables': ['ts','albedo','albvisdf','albvisdr','swtdn','swgdn'],
            'dataset': 'tavg1_2d_rad_Nx',
            'shortname': 'MAT1NXRAD'
        })
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
