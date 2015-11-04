import datetime
from urllib2 import urlopen, URLError, HTTPError
import contextlib
import logging
import datetime

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
