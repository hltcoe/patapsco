import setuptools

with open("patapsco/__version__.py") as fp:
    ns = {}
    exec(fp.read(), ns)

with open("README.md") as fp:
    long_description = fp.read()

setuptools.setup(
    name="patapsco",
    version=ns['__version__'],
    description="Cross Language Information Retrieval pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="BSD",
    python_requires=">=3.6",
    packages=setuptools.find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[
        "beautifulsoup4",
        "more-itertools",
        "numpy",
        "pydantic>=1.8.1",
        "pytrec_eval",
        "pyyaml",
        "scriptnorm",
        "sqlitedict",
        "stanza==1.2.0",
    ],
    extras_require={
        "dev": ["pytest", "flake8", "autopep8"]
    },
    entry_points={
        "console_scripts": ["patapsco = patapsco.__main__:main"]
    }
)
