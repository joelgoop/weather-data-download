from setuptools import setup

setup(
    name='weather-get',
    version='0.1',
    packages=['wdata'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        wdata=wdata.main:cli
    ''',
)