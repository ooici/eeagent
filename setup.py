#!/usr/bin/env pythonv

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

import sys
version = '0.1.0'

if float("%d.%d" % sys.version_info[:2]) < 2.5:
    sys.stderr.write("Your Python version %d.%d.%d is not supported.\n" % sys.version_info[:3])
    sys.stderr.write("eeagent requires Python 2.5 or newer.\n")
    sys.exit(1)

setup(name='eeagent',
      version=version,
      description='Execution Engine Agent',
      author='Nimbus Development Team',
      author_email='workspace-user@globus.org',
      url='http://www.nimbusproject.org/',
      packages=find_packages(),
      package_data={'eeagent': ['config/*.yml']},
      dependency_links=['http://sddevrepo.oceanobservatories.org/releases'],
      keywords = "OOI Execution Agent",
      long_description="""Some other time""",
      license="Apache2",
      install_requires = ["dashi", "pidantic", "simplejson"],
      entry_points = {
        'console_scripts': [
            'eeagent = eeagent.agent:main',
            'eeagentclient = eeagent.client:main',
        ],},

      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python',
          'Topic :: System :: Clustering',
          'Topic :: System :: Distributed Computing',
          ],
     )
