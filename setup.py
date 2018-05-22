#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(name='mulder',
      version='1.0',
      description='MULDER - Querying the Linked Data Web by Bridging RDF Molecule Templates ',
      author='Kemele M. Endris',
      author_email='endris@L3s.de',
      url='https://github.com/SDM-TIB/MULDER',
      scripts=['start_experiment.py', 'start_dief_experiment.py', 'scripts/collect_rdfmts.py', "runQueries.sh", "EndpointService.py"],
      packages=find_packages(exclude=['docs']),
      install_requires=["ply", "flask"],
      include_package_data=True,
      license='GNU/GPL v2'
     )
