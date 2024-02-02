import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as req_file:
    requirements = req_file.read().splitlines()

setuptools.setup(
    name="brazilian-data-market",
    version="1.0.0",
    author="Andesson Reis",
    author_email="andessonreys@gmail.com",
    description="Spark application for the Brazilian Data Market",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Andessonreis/Brazilian-Data-Market",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
