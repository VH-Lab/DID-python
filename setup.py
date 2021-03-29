import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="did",
    version="0.0.1",
    author="Squishymedia",
    author_email="info@squishymedia.com",
    description="DID Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=['sqlalchemy', 
					    'psycopg2-binary', 
					    'neo', 
					    'alchemy-mock', 
					    'sqlalchemy-utils', 
					    'blake3', 
					    'astropy',
					    'pymongo']
)
