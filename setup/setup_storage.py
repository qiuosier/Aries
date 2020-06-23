"""Build Script for setuptools

This build script must be executed outside of the Aries directory.

See Also: https://packaging.python.org/tutorials/packaging-projects/
"""
import setuptools
from Aries.setup import setup


setuptools.setup(
    name="Aries-storage",
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description="Read and write files on Google Cloud Storage and Amazon S3 as if they are on local computer.",
    url="https://github.com/qiuosier/Aries",
    version=setup.get_version(),
    long_description=setup.get_description("Aries/docs/storage.md"),
    install_requires=setup.get_requirements("Aries/setup/requirements_storage.txt", req_aries=True),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(include=(
        "Aries.storage",
    )),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
