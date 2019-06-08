from setuptools import setup, find_packages

setup(
    name="fonz",
    version="0.0.1",
    py_modules=["fonz"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "requests",
        "PyYAML",
        "pytest",
        "requests-mock",
        "coverage",
        "mypy",
        "pycodestyle",
        "pytest-cov",
        "coverage",
    ],
    entry_points="""
        [console_scripts]
        fonz=fonz.cli:cli
    """,
)
