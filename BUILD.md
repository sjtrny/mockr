# Build Instructions for pypi

https://packaging.python.org/guides/distributing-packages-using-setuptools/

1. Update version number

2. Make universal wheel

    `python setup.py bdist_wheel --universal`

3. Upload to pypi
    `twine upload dist/*`

