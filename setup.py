import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as rq:
	requirements = rq.read().split('\n')

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
    install_requires=requirements
)
