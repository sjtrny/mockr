# Build Instructions for pypi

https://packaging.python.org/guides/distributing-packages-using-setuptools/

1. Update version number

2. Make universal wheel

    `python setup.py bdist_wheel`

3. Upload to pypi
    `twine upload dist/*`
    
    
# Future expansion

https://softwareengineering.stackexchange.com/questions/243044/single-python-file-distribution-module-or-package
