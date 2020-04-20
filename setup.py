"""Build Script for setuptools

This build script must be executed outside of the Aries directory.

See Also: https://packaging.python.org/tutorials/packaging-projects/
"""
import setuptools
import os
from Aries.files import Markdown

with open("Aries/README.md", "r") as f:
    long_description = f.read()
    long_description = Markdown.from_text(
        long_description
    ).make_links_absolute("https://github.com/qiuosier/Aries/blob/master/")


with open("Aries/requirements.txt", "r") as f:
    requirements = f.read().split("\n")
    requirements = [r.strip() for r in requirements if r.strip()]

release_version = str(os.popen('cd Aries && git tag | tail -1').read()).strip()
commit_version = str(os.popen('cd Aries && git rev-list --count master').read()).strip()

setuptools.setup(
    name="Astrology-Aries",
    version="%s%s" % (release_version, commit_version),
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description="Python package providing shortcuts to tasks like "
                "accessing files on the cloud, running background tasks, configuring logging, etc.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/qiuosier/Aries",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
