import os
import re

from setuptools import find_packages, setup

with open('./cq/__init__.py') as f:
    exec(re.search(r'VERSION = .*', f.read(), re.DOTALL).group())

setup(
    name='django-cq',
    version=__version__,  # noqa
    author='Luke Hodkinson',
    author_email='luke.hodkinson@uptickhq.com',
    maintainer='Uptick',
    maintainer_email='dev@uptickhq.com',
    url='https://github.com/uptick/django-cq',
    description='Distributed tasks for Django Channels.',
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    package_data={'': ['*.txt', '*.js', '*.html', '*.*']},
    install_requires=[
        'setuptools',
        'six',
        'croniter',
        'channels>=2.1.0',
        'channels_redis>=2.3.0',
    ],
)
