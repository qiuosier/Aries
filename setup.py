import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Astrology-Aries",
    version="0.0.1.dev1",
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description="The Aries Library",
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