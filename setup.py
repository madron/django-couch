import os
from codecs import open
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'requirements', 'common.txt')) as f:
    requirements = f.read().splitlines()

with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='django-couch',
    version='0.1',
    url="http://bitbucket.org/massimilianoravelli/django-couch",
    description='Couchdb django api.',
    long_description=long_description,
    author='Massimiliano Ravelli',
    author_email='massimiliano.ravelli@gmail.com',
    license='MIT',
    keywords='django couchdb'.split(),
    platforms='any',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities',
    ],
    zip_safe=False,
    packages=find_packages(),
    install_requires=requirements,
)
