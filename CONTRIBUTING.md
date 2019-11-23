# Contributing to spectacles
## Setting up for local development

Follow [these standard instructions](https://opensource.guide/how-to-contribute/#opening-a-pull-request) to get your project set up for development. In a nutshell, you should:

* Fork the repository on GitHub
* Clone your fork to your local machine
* Create a new local branch off `master` using the `feature/feature-name` branch naming convention
* Create a Python virtual environment and install dependencies with `pip install -r requirements.txt`

Once your local repository is set up, develop away on your feature! Double-check that you've included the following:

* [Tests](https://docs.pytest.org/en/latest/) in tests/ for any new code that you introduce
* [Type hints](https://docs.python.org/3/library/typing.html) for all input arguments and returned outputs

## Test requirements for submission

All pull requests must pass the following checks:
* [`pytest`](https://docs.pytest.org/en/latest/) to run unit and functional Python tests
* [`mypy`](http://mypy-lang.org/) to check types
* [`flake8`](http://flake8.pycqa.org/en/latest/) to enforce the Python style guide
* [`black`](https://black.readthedocs.io/en/stable/) to auto-format Python code

If you want to test your code locally before submitting a pull request, you can find the exact code that runs each of these checks in our [CI configuration file](.circleci/config.yml).

## Submitting a pull request

Once you've completed development, testing, docstrings, and type hinting, you're ready to submit a pull request. Create a pull request from the feature branch in your fork to `master` in the main repository.

Reference any relevant issues in your PR. If your PR closes an issue, include it (e.g. "Closes #19") so the issue will be auto-closed when the PR is merged.
