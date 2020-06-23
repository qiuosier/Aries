"""Build Script for setuptools

This build script must be executed outside of the Aries directory.

See Also: https://packaging.python.org/tutorials/packaging-projects/
"""
import setuptools
from Aries.setup import setup


setuptools.setup(
    name="Aries-core",
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description="Python package providing shortcuts to small tasks like string manipulation, "
                "running background tasks, configuring logging, accessing web API etc.",
    url="https://github.com/qiuosier/Aries",
    version=setup.get_version(),
    long_description=setup.get_description("Aries/README.md"),
    install_requires=setup.get_requirements("Aries/setup/requirements_core.txt"),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=(
        "Aries.setup",
        "Aries.gcp",
        "Aries.statistics",
        "Aries.storage",
        "Aries.tests",
        "Aries.visual",
    )),
    package_data={
        "Aries": ["assets/*"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
