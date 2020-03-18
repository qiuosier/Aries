import setuptools
import os
from Aries.files import Markdown

with open("Aries/README.md", "r") as fh:
    long_description = fh.read()
    long_description = Markdown.from_text(long_description).make_links_absolute("https://github.com/qiuosier/Aries/blob/master/")

version = os.popen('cd Aries && git rev-list --count master').read()

setuptools.setup(
    name="Astrology-Aries",
    version="0.0.1.dev%s" % version,
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description="Python package providing shortcuts to tasks like "
                "accessing files on the cloud, running background tasks, configuring logging, etc.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/qiuosier/Aries",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
