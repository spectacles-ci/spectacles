from setuptools import setup, find_packages

setup(
    name="fonz",
    version="0.0.1",
    py_modules=["fonz"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=["Click", "requests", "PyYAML", "colorama", "backoff", "aiohttp"],
    tests_require=[
        "pytest",
        "pytest-cov",
        "requests_mock",
        "mypy",
        "coverage",
        "pytest-asyncio",
        "asynctest",
        "argparse",
    ],
    entry_points="""
        [console_scripts]
        fonz=fonz.cli:main
    """,
)
