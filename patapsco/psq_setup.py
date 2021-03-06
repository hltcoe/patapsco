# !! Beware, this is a monkey patch to allow adding PSQ Java functionality to Pyserini without having to rewrite !!
# !! portions of the search package !!

import glob
import os
from pathlib import Path

import jnius_config
import pyserini.setup


def configure_classpath_psq():
    anserini_root = str(Path(pyserini.__file__).parent / 'resources/jars/')
    paths = glob.glob(os.path.join(anserini_root, 'anserini-*-fatjar.jar'))
    if not paths:
        raise Exception('No matching jar file found in {}'.format(os.path.abspath(anserini_root)))

    latest = max(paths, key=os.path.getctime)
    jnius_config.add_classpath(latest)
    psq_path = (Path(__file__).parent / 'resources' / 'jars').glob('psq*.jar')
    if not psq_path:
        raise Exception('No matching jar file found in resources/jars')

    jnius_config.add_classpath(str(list(psq_path)[0]))


def skip_setting_classpath(anserini_root="."):
    pass


# !! Beware, this is a monkey patch to allow adding PSQ Java functionality to Pyserini without having to rewrite !!
# !! portions of the search package !!
pyserini.setup.configure_classpath = skip_setting_classpath

configure_classpath_psq()
