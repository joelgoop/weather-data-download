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
@click.argument('years',nargs=-1,type=int)
@click.option('--dest','-d', type=click.Path(exists=True),required=True)
@click.option('--source','-s',type=click.Choice(['merra']),required=True,default='merra')
@click.option('--datatype','-t',type=click.Choice(['wind', 'solar']),required=True)
@click.option('--filefmt','-f',type=click.Choice(['nc', 'hdf']),default='hdf')
def download(source,years,**kwargs):
    year_list = years
    try:
        # If years is of length 2 interpret as start/end year
        start_year,end_year = years
        if end_year >= start_year:
            year_list = range(start_year,end_year+1)
    except ValueError:
        pass

    if source=='merra':
        import merra
        logger.info('Downloading {} data in {} format from MERRA for years {}.'.format(kwargs['datatype'],kwargs['filefmt'],', '.join(map(str,year_list))))
        merra.download(year_list,**kwargs)
    else:
        logger.error('Unknown source!')


@cli.command()
@click.option('--source','-s',type=click.Path(exists=True),required=True)
@click.option('--dest','-d',type=click.Path(exists=True),required=True)
@click.option('--dataformat','-f',type=click.Choice(['merra']),required=True,default='merra')
def clean(dataformat,**kwargs):
    if dataformat=='merra':
        import merra
        logger.info('Cleaning data from MERRA')
