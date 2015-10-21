import click
import os
import contextlib
import logging, logging.config
import datetime
from urllib2 import urlopen, URLError, HTTPError
from urllib import urlencode
from itertools import product,chain


logger = logging.getLogger(__name__)

def daterange(start_date, end_date):
    """
    Generator to iterate over range of dates.

    Args:
        start_date (datetime.date): date object for starting point
        end_date (datetime.date): date object for end point (not included)

    Yields:
        datetime.date: the next date between start and end dates
    """
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(days=n)


def dlfile(url,fname):
    """
    Download url content and save to local file.

    Args:
        url (str): URL string to download
        fname (str): path to local file 
    """
    # Open the url
    try:
        logger.debug("Attempting to download: {}".format(url))
        with contextlib.closing(urlopen(url)) as f:

            # Open our local file for writing
            with open(fname, "wb") as local_file:
                logger.debug("Attempting to read data and write to '{}'.".format(fname))
                local_file.write(f.read())
        return True

    # Handle errors
    except HTTPError as e:
        logger.exception("HTTP Error {}. {}".format(e.code,url)) 
    except URLError as e:
        logger.exception("URL Error: {}. {}".format(e.reason,url))


def create_merra_url(date,bbox=(29.531,-15.469,76.641,33.75),variables=None):
    """
    Create URL string to download MERRA data from tavg1_2d_slv_Nx for specific date.

    Args:
        date (datetime.date): date for which to get data
        bbox (4-tuple of floats): bounding box in lat/long as tuple (lower lat, lower 
            long, upper lat, upper long) default is main Europe
        vars (list): names for variables in MERRA

    Returns:
        A tuple with URL string and label.
    """
    # Default variables: u and v for 2, 10, and 50 meters height
    if variables is None:
        variables = [d+h+'m' for d,h in product(['v','u'],['2','10','50'])]

    bbox_str = '{},{},{},{}'.format(*bbox)
    logger.debug("Create URL to get {} for {} within ({}).".format(','.join(variables),date,bbox_str))

    # MERRA200 from Jan 1 1992
    merra200_breakpoint = datetime.date(1992,1,1)
    # MERRA300 from Jan 1 2001
    merra300_breakpoint = datetime.date(2001,1,1)

    # Set MERRA version based on date
    if date < merra200_breakpoint:
        merra_version = 100
    elif date < merra300_breakpoint:
        merra_version = 200
    else:
        merra_version = 300

    merra_args = {
        'BBOX': bbox_str,
        'FILENAME': r'/data/s4pa/MERRA/MAT1NXSLV.5.2.0/{date.year}/{date.month:02d}/MERRA{merra_version}.prod.assim.tavg1_2d_slv_Nx.{date.year}{date.month:02d}{date.day:02d}.hdf'.format(date=date,merra_version=merra_version),
        'FORMAT': 'SERGLw',
        'LABEL': 'MERRA{merra_version}.prod.assim.tavg1_2d_slv_Nx.{date.year}{date.month:02d}{date.day:02d}.SUB.hdf'.format(date=date,merra_version=merra_version),
        'VARIABLES': ','.join(variables),
        'VERSION': '1.02',
        'SHORTNAME': 'MAT1NXSLV',
        'SERVICE': 'SUBSET_LATS4D',
    }

    return (r"http://goldsmr2.sci.gsfc.nasa.gov/daac-bin/OTF/HTTP_services.cgi?{}".format(urlencode(merra_args)),merra_args['LABEL'])


def dlmerra(date,dest,**kwargs):
    """
    Download MERRA data for specific date into target directory.

    Args:
        date (datetime.date): date for which to download data
        dest (str): path to destination directory
        kwargs: keyword arguments to be sent to create_merra_url
    """
    url,label = create_merra_url(date,**kwargs)
    # Check if label is filename with acceptable extension or add
    if 'hdf' not in os.path.splitext(label)[1]:
        label += '.hdf'
    target_file = os.path.join(dest,label)

    logger.debug('Attempting to download. URL:\n{}\nTarget file: {}'.format(url,target_file))
    dlfile(url,target_file)


@click.group()
@click.option('--debug','-d',is_flag=True,help='Show debug messages.')
def cli(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)-8s] %(message)s",
                        datefmt="%H:%M")

@cli.command()
@click.argument('years',nargs=-1,type=int)
@click.argument('dest', type=click.Path(exists=True))
def download(years,dest):
    dates = chain(*[daterange(start_date=datetime.date(year,1,1),end_date=datetime.date(year+1,1,1)) for year in years])
    for date in dates:
        dlmerra(date,dest)
        logger.info('Downloaded for {}.'.format(date))