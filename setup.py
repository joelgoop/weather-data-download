from setuptools import setup

setup(
    name='merra-get',
    version='0.1',
    py_modules=['merra_get'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        merra-get=merra_get:cli
    ''',
)