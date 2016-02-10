import datetime
from urllib2 import urlopen, URLError, HTTPError
import contextlib
import logging
import datetime
import threading
import Queue
import time
from functools import wraps

logger = logging.getLogger(__name__)

def retry(exc, tries=4, delay=3, backoff=1.2):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    Args:
        exc (Exception): exception to check for
        tries (int): number of times to retry
        delay (number): seconds to wait 
        backoff (number): 
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exc as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    logger.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

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

def hourrange(start_time, end_time):
    """
    Generator to iterate over range of hours.

    Args:
        start_time (datetime.datetime): datetime object for starting point
        end_time (datetime.datetime): datetime object for end point (not included)

    Yields:
        datetime.datetime: the next hour between start and end times
    """
    cur_time = start_time
    while cur_time<end_time:
        yield cur_time
        cur_time += datetime.timedelta(hours=1)


def to_timestamp(dt):
    """
    Convert python datetime to timestamp.

    Args:
        dt (datetime.datetime): datetime object to convert
    Returns:
        int: timestamp
    """
    return int((dt-datetime.datetime(1970,1,1,0,0)).total_seconds())

@retry(HTTPError,10)
def dlfile(url,fname):
    """
    Download url content and save to local file.

    Args:
        url (str): URL string to download
        fname (str): path to local file 
    """
    # Open the url
    logger.debug("Attempting to download: {}".format(url))
    with contextlib.closing(urlopen(url)) as f:

        # Open our local file for writing
        with open(fname, "wb") as local_file:
            logger.debug("Attempting to read data and write to '{}'.".format(fname))
            local_file.write(f.read())
    return True


class Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        threading.Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()
    
    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try: func(*args, **kargs)
            except Exception, e: print e
            self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue.Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()