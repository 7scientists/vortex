from distutils.core import setup
from setuptools import find_packages

setup(name='vortex',
version='0.0.1',
author=u'Andreas Dewes - 7scientists GmbH',
author_email = 'andreas.dewes@7scientists.com',
license = '',
install_requires = """""",
entry_points = {
    },
url='https://github.com/7scientists/vortex',
packages=find_packages(),
zip_safe = False,
description='Vortex: A graph database abstraction layer.',
long_description="""
Vortex is a graph database abstraction layer.
"""
)
