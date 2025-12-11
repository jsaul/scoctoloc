from setuptools import setup, find_packages

setup(
    name="scoctoloc",

    description="Real-time event association/location for SeisComP using PyOcto",

    version="0.0.6",

    url='https://github.com/jsaul/scoctoloc',

    author='Joachim Saul',

    author_email='saul@gfz.de',

    license='AGPLv3',

    keywords='SeisComP, PyOcto, real-time seismology, earthquake location, phase association',

    provides=["scocto"],

    install_requires=['pyocto', 'seiscomp', 'numpy'],

    python_requires='>=3',

)
