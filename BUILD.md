# Build Instructions for pypi

1. Update version number

2. Make universal wheel

    `python setup.py bdist_wheel --universal`

3. Upload to pypi
    `twine upload dist/*`

