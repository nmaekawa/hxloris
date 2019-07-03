#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from setuptools import setup
from setuptools import find_packages


project_name='hxloris'

def get_version(*file_paths):
    """Retrieves the version from [your_package]/__init__.py"""
    filename = os.path.join(os.path.dirname(__file__), *file_paths)
    version_file = open(filename).read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


version = get_version(project_name, "__init__.py")


with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'boto3',
    # hxloris depends on https://github.com/loris-imageserver/loris.git
    # but setup.py not always can install pkgs from git repos...
    #
    # install via pip -r requirementst.txt
    #
]

test_requirements = [
    'pytest',
    'flake8',
]

setup(
    name=project_name,
    version=version,
    description='hx add-ons to loris image server',
    long_description=readme,
    author='nmaekawa',
    author_email='nmaekawa@g.harvard.edu',
    url='https://github.com/nmaekawa/hxloris',
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='loris s3resolver ' + project_name,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
)
