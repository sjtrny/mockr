# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

# Get the long description from the README file
with open(path.join(path.dirname(__file__), 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name = "mockmr",
    version = "0.31",
    description = "A Python library for prototyping MapReduce jobs",
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/sjtrny/mockmr',
    author = 'Stephen Tierney',
    author_email = 'sjtrny@gmail.com',
    keywords = 'mockmr mapreduce map reduce education',
    packages=find_packages(exclude=['examples', 'docs', 'build', 'dist']),
    install_requires=['pandas'],
    python_requires='>=3',
    classifiers = [
        'License :: OSI Approved :: MIT License',
      ]
)
