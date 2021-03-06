import click
import logging, logging.config
import os

logger = logging.getLogger('weather-data-download')
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"
LOG_DATEFMT = "%H:%M:%S"

@click.group()
@click.option('--debug','-d',is_flag=True,help='Show debug messages.')
def cli(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATEFMT)


@cli.command(help="download wind or solar data")
@click.argument('datatype',type=click.Choice(['wind', 'solar']))
@click.argument('years',nargs=-1,type=int,required=True)
@click.option('--dest','-d', type=click.Path(exists=True,file_okay=False),required=True,
    help='destination folder')
@click.option('--filefmt','-f',type=click.Choice(['nc', 'hdf','nc4','default']),default='default',
    help='file format (default set by datasource)')
@click.option('--datasource','-ds',type=click.Choice(['merra','merra2']),default='merra',
    help='source for data (default \'merra\')')
@click.option('--logfile/--no-logfile',default=True,
    help='write log to file in target directory (default True)')
@click.option('--skip-existing/--no-skip-existing',default=True,
    help='skip downloading if target already exists (default True)')
def download(years,datasource,logfile,dest,**kwargs):
    if logfile:
        fh = logging.FileHandler(os.path.join(dest,'download.log'))
        fh.setFormatter(logging.Formatter(LOG_FORMAT,datefmt='%Y-%m-%d '+LOG_DATEFMT))
        logger.addHandler(fh)

    logger.debug('Years: {}\nDatasource: {}\nLogfile: {}\nDest: {}\nKeyword args: {}'.format(
        years,datasource,logfile,dest,kwargs))

    year_list = years
    try:
        # If years is of length 2 interpret as start/end year
        start_year,end_year = years
        if end_year >= start_year:
            year_list = range(start_year,end_year+1)

        logger.debug('Test1')
    except ValueError:
        pass

    logger.debug('Test2')

    if 'merra' in datasource:
        import merra
        logger.info('Downloading {} data in {} format from MERRA for years {}.'.format(kwargs['datatype'],kwargs['filefmt'],', '.join(map(str,year_list))))
        merra.download(year_list,datasource,dest,**kwargs)
    else:
        logger.error('Unknown source!')


@cli.command(help="aggregate and create time index")
@click.argument('datasource',type=click.Choice(['merra','merra2']),default='merra')
@click.option('--source','-s',type=click.Path(exists=True),required=True)
@click.option('--dest','-d',type=click.Path(exists=True),required=True)
@click.option('--year','-y',type=int,required=False)
@click.option('--datatype','-t',type=click.Choice(['wind', 'solar']),required=False)
@click.option('--skip-existing/--no-skip-existing',default=True,
    help='skip cleaning if output file already exists (default True)')
def clean(datasource,**kwargs):
    if 'merra' in datasource:
        import merra
    if datasource=='merra':
        logger.info('Cleaning data from MERRA')
        merra.clean_merra(**kwargs)
    elif datasource=='merra2':
        logger.info('Cleaning data from MERRA2')
        merra.clean_merra2(**kwargs)
