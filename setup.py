from pathlib import Path
from setuptools import setup, find_packages
from spectacles import __version__

here = Path(__file__).parent.resolve()

# Get the long description from the README file
with (here / "README.md").open(encoding="utf-8") as file:
    long_description = file.read()

setup(
    name="spectacles",
    description="A command-line, continuous integration tool for Looker and LookML.",
    version=__version__,
    py_modules=["spectacles"],
    packages=find_packages(exclude=["docs", "tests*", "scripts"]),
    include_package_data=True,
    install_requires=[
        "requests",
        "PyYAML",
        "colorama",
        "backoff",
        "analytics-python",
        "typing-extensions",
        "tabulate",
    ],
    tests_require=[
        "black",
        "mypy",
        "pytest",
        "pytest-cov",
        "pytest-recording",
        "pytest-randomly",
        "flake8",
        "pre-commit",
        "vcrpy",
        "jsonschema",
        "PyGithub",
    ],
    entry_points={"console_scripts": ["spectacles = spectacles.cli:main"]},
    author="Spectacles",
    author_email="hello@spectacles.dev",
    url="https://github.com/spectacles-ci/spectacles",
    download_url="https://github.com/spectacles-ci/spectacles/tarball/" + __version__,
    long_description=long_description,
    long_description_content_type="text/markdown",
)
