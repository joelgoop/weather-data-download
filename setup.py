from setuptools import setup

setup(
    name='weather-get',
    version='0.1',
    py_modules=['weather_get'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        wdata=weather_get:cli
    ''',
)