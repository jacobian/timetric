import os
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name = "timetric",
    version = "1.0",
    description = "Client library for Timetric (http://timetric.com/)",
    long_description = open(os.path.join(os.path.dirname(__file__), 'README')).read(),
    author = 'Jacob Kaplan-Moss',
    author_email = 'jacob@jaobian.org',
    url = 'http://github.com/jacobian/timetric',
    packages = ['timetric'],
    install_requires = [
        'httplib2',
        'python-dateutil',
        'oauth',
        'simplejson',
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet',
    ]
)