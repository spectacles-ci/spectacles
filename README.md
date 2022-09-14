<img src="https://github.com/spectacles-ci/spectacles/raw/master/docs/img/logo.png" width="600">

[![CircleCI](https://circleci.com/gh/spectacles-ci/spectacles.svg?style=svg)](https://circleci.com/gh/spectacles-ci/spectacles)
[![downloads](https://img.shields.io/pypi/dm/spectacles)](https://img.shields.io/pypi/dm/spectacles)

## What is Spectacles?

**[Spectacles](https://spectacles.dev/?utm_source=github&utm_medium=readme) is a continuous integration tool for Looker and LookML.** Spectacles runs **validators** which perform a range of tests on your Looker instance and your LookML. Each validator interacts with the Looker API to run tests that ensure your Looker instance is running smoothly.

From the command line, you can run the following validators as subcommands (e.g. `spectacles sql`):

✅ [**SQL** validation](https://docs.spectacles.dev/cli/tutorials/validators#the-sql-validator) - tests the `sql` field of each dimension for database errors

✅ [**Assert** validation](https://docs.spectacles.dev/cli/tutorials/validators#the-assert-validator) - runs [Looker data tests](https://docs.looker.com/reference/model-params/test)

✅ [**Content** validation](https://docs.spectacles.dev/cli/tutorials/validators#the-content-validator) - tests for errors in Looks and Dashboards

✅ [**LookML** validation](https://docs.spectacles.dev/cli/tutorials/validators/#the-lookml-validator) - runs [LookML validator](https://cloud.google.com/looker/docs/lookml-validation)

## Installation

Spectacles CLI is distributed on PyPi and is easy to install with pip:

```bash
pip install spectacles
```

> 📣 You can also use Spectacles as a **full-service web application**! Check out **[our website](https://spectacles.dev/?utm_source=github&utm_medium=readme)** to learn more.

## Documentation

You can find detailed documentation for the CLI and web app on our docs page: [docs.spectacles.dev](https://docs.spectacles.dev/cli/tutorials/getting-started).

## Why we built this

Occasionally, when we make changes to LookML or our data warehouse, we break downstream experiences in Looker:

* Changing the name of a database column without changing the corresponding `sql` field in our Looker view, leaving our users with a database error when using that field
* Adding an invalid join to an explore that fans out our data, inflating a key metric that drives our business without realising
* Editing LookML without remembering to check the Content Validator for errors, disrupting Dashboards and Looks that our users rely on
* Giving a new dimension a confusing name, causing other developers in our team to spend extra time trying to figure out how it should be used

**We believe in the power of testing and continuous integration for analytics.** We believe that automated tests should catch these errors before they're ever pushed to production.

We wanted a single tool to perform these checks for us, so we built Spectacles to enhance the business intelligence layer of analytics CI pipelines.

## Community

Have a question or just want to chat Spectacles or Looker? [Join us in Slack.](https://join.slack.com/t/spectacles-ci/shared_invite/zt-akmm4mo6-XnPcUUaG3Z5~giRc_5JaUQ)
