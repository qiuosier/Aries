"""Build Script for setuptools

This build script must be executed outside of the Aries directory.

See Also: https://packaging.python.org/tutorials/packaging-projects/
"""
import os
from ..files import Markdown


def get_version():
    # Determine the package version base on the last release tag and total number of first parent commits on master
    release_version = str(os.popen('cd Aries && git tag | tail -1').read()).strip()
    commit_version = str(os.popen('cd Aries && git rev-list --count master --first-parent').read()).strip()
    minor_version = release_version.rsplit(".", 1)[-1]
    if minor_version.isdigit():
        version = "%s.%s" % (release_version, commit_version)
    else:
        version = "%s%s" % (release_version, commit_version)
    return version


def get_description(readme):
    with open(readme, "r") as f:
        long_description = f.read()
        long_description = Markdown.from_text(
            long_description
        ).make_links_absolute("https://github.com/qiuosier/Aries/blob/master/")
        return long_description


def get_requirements(req_path, req_aries=False):
    with open(req_path, "r") as f:
        requirements = f.read().split("\n")
        requirements = [r.strip() for r in requirements if r.strip()]
        if req_aries:
            requirements.insert(0, "Aries-core==%s" % get_version())
        return requirements
