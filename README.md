# Data Interface Database

The did-python library is a versioned database API built to manage [DID documents](https://github.com/VH-Lab/DID-matlab/wiki/Discussion-on-DID-document-core) and associated binary data.

## Usage

Basic usage is recommended via [Jupyter Notebook](https://jupyter.org/). Any Jupyter install should work but [Anaconda](https://www.anaconda.com/distribution/) (Python 3) is an easy way to get started with Jupyter.

## Package manager
did-python uses pipenv for package management, it can be installed with pip.

## Jupyter Notebook Quickstart

```shell
# Install dependencies (use the --dev flag if you plan to run the linter, test suite, or docs)
$ pipenv install
# Activate virtualenv
$ pipenv shell
# Verify virtual environment (OPTIONAL)
(ndi-python) $ which jupyter
# Start Jupyter Notebook
(ndi-python) $ jupyter notebook
```

## Developer Notes

The Python package makes use of [native namespace packages available in Python 3.3 and later](https://packaging.python.org/guides/packaging-namespace-packages/#native-namespace-packages).

To install the did pacakge in editable mode from the local project path, run the following commands

```shell
git clone 'https://github.com/VH-Lab/DID-python.git'
cd ./DID-python
pip install -e '.' 

# import did from command line 

python3

Python 3.8.8 (default, Feb 27 2021, 02:19:17) 
[Clang 12.0.0 (clang-1200.0.32.29)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> import did
```

The test suite can be run from the pipenv shell with `python -m pytest`.

## Documentation

This library is documented in the Jupyter Notebook at ./example/Core_API.