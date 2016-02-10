import click
import logging, logging.config

logger = logging.getLogger(__name__)


@click.group()
@click.option('--debug','-d',is_flag=True,help='Show debug messages.')
def cli(debug):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level,
                        format="%(asctime)s [%(levelname)-8s] %(message)s",
                        datefmt="%H:%M:%S")


@cli.command()
@click.argument('datatype',type=click.Choice(['wind', 'solar']))
@click.argument('years',nargs=-1,type=int)
@click.option('--dest','-d', type=click.Path(exists=True,file_okay=False),required=True,
    help='destination folder')
@click.option('--filefmt','-f',type=click.Choice(['nc', 'hdf']),default='hdf',
    help='file format (default \'hdf\')')
@click.option('--datasource','-ds',type=click.Choice(['merra']),default='merra',
    help='source for data (default \'merra\')')
def download(years,datasource,**kwargs):
    year_list = years
    try:
        # If years is of length 2 interpret as start/end year
        start_year,end_year = years
        if end_year >= start_year:
            year_list = range(start_year,end_year+1)
    except ValueError:
        pass

    if datasource=='merra':
        import merra
        logger.info('Downloading {} data in {} format from MERRA for years {}.'.format(kwargs['datatype'],kwargs['filefmt'],', '.join(map(str,year_list))))
        merra.download(year_list,**kwargs)
    else:
        logger.error('Unknown source!')


@cli.command()
@click.argument('datasource',type=click.Choice(['merra']),default='merra')
@click.option('--source','-s',type=click.Path(exists=True),required=True)
@click.option('--dest','-d',type=click.Path(exists=True),required=True)
@click.option('--year','-y',type=int,required=False)
@click.option('--datatype','-t',type=click.Choice(['wind', 'solar']),required=False)
def clean(datasource,**kwargs):
    if datasource=='merra':
        import merra
        logger.info('Cleaning data from MERRA')

        merra.clean(**kwargs)
