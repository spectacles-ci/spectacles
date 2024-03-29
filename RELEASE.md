# Releasing a new version of Spectacles

## Follow semver

Spectacles follows semantic versioning. Familiarize yourself with [semver](https://semver.org/) before creating a new version of Spectacles. Non-development releases should be approved by all project maintainers.

## Write a draft release on GitHub

_Doesn't apply to development versions (alpha, beta, or release candidate)_

1. The release notes should have two sections: **Features** for new functionality and **Fixes** for bugfixes or internal changes.

1. Include links for each feature or fix to any relevant GitHub issues or documentation sections. See [previous releases](https://github.com/spectacles-ci/spectacles/releases/tag/v0.1.1) for examples.

1. Save the release as a draft. If you'd like, you can draft and edit the release notes as you merge features and fixes, it's not public.

## Get code and docs ready

All code to be released should be merged into `master`. If the release is a major or minor release, there should be a corresponding, approved PR on the docs repository ready to be merged that covers any notable new functionality or API changes.

## Bump the version

The version number for Spectacles is stored in the `__init__.py` file in `spectacles/` as the argument `__version__`.

Change the version there and commit it with the message "Bump version [CURRENT_VERSION] > [NEW_VERSION]". You can commit this directly to `master` or create a separate branch (e.g. `release/0.2.2`) for this change.

## Build distributions

To build distributions, run the following command from the root of the Spectacles repository on the `master` branch.

```bash
python -m build --sdist --wheel
```

This command will build distributions in the `dist` directory within the Spectacles repository root.

## Deploy to PyPi

To deploy a relase, you must have an account on PyPi with at least Maintainer role on the [Spectacles project](https://pypi.org/manage/project/spectacles/collaboration/).

Next, install [`twine`](https://twine.readthedocs.io/en/latest/#installation) and optionally [set up Keyring](https://twine.readthedocs.io/en/latest/#keyring-support) so you don't have to enter your username and password each time.

To deploy to production PyPi, use this command:

```bash
twine upload --skip-existing dist/*
```

To deploy to the test PyPi instance, use this command instead:

```bash
twine upload --skip-existing --repository testpypi dist/*
```

## Publish draft GitHub release

_Doesn't apply to development versions (alpha, beta, or release candidate)_

Publish the draft release. This will generate a corresponding tag on the HEAD commit of `master`.

## Merge the docs PR and announce

_Doesn't apply to development versions (alpha, beta, or release candidate)_

At this point, the release is ready to share with the world. Merge any corresponding docs PR and announce the release in Slack!
