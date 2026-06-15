import os
import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install


def _copy_etc_to_seiscomp_root():
    seiscomp_root = os.environ.get("SEISCOMP_ROOT")
    if not seiscomp_root:
        print("SEISCOMP_ROOT is not set; skipping etc file installation")
        return

    source = Path(__file__).resolve().parent / "etc"
    target = Path(seiscomp_root) / "etc"

    if not source.is_dir():
        print(f"No etc directory found at {source}; skipping")
        return

    target.mkdir(parents=True, exist_ok=True)

    for root, _, files in os.walk(source):
        relative_root = Path(root).relative_to(source)
        destination_root = target / relative_root
        destination_root.mkdir(parents=True, exist_ok=True)
        for filename in files:
            shutil.copy2(Path(root) / filename, destination_root / filename)

    print(f"Installed etc files from {source} to {target}")


class InstallCommand(install):
    def run(self):
        super().run()
        _copy_etc_to_seiscomp_root()


class DevelopCommand(develop):
    def run(self):
        super().run()
        _copy_etc_to_seiscomp_root()

setup(
    name="scoctoloc",

    description="Real-time event association/location for SeisComP using PyOcto",

    version="0.0.7",

    url='https://github.com/jsaul/scoctoloc',

    author='Joachim Saul',

    author_email='saul@gfz.de',

    license='AGPLv3',

    keywords='SeisComP, PyOcto, real-time seismology, earthquake location, phase association',

    provides=["scocto"],

    install_requires=['pyocto', 'pyrocko'],

    python_requires='>=3',

    cmdclass={
        'install': InstallCommand,
        'develop': DevelopCommand,
    },

)
