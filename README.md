# spectacles
[![CircleCI](https://circleci.com/gh/dbanalyticsco/spectacles/tree/master.svg?style=svg)](https://circleci.com/gh/dbanalyticsco/spectacles/tree/master)
[![codecov](https://codecov.io/gh/dbanalyticsco/spectacles/branch/master/graph/badge.svg)](https://codecov.io/gh/dbanalyticsco/spectacles)

`spectacles` helps verify that your Looker instance functions as expected. `spectacles` is a diagnostic tool that checks all explore **dimensions for database errors** and **validates Looks and Dashboards**.

Add `spectacles` to your **CI pipeline** to check for unexpected side effects from database or LookML changes.

## Why we built this
Occasionally, when we make changes to LookML or underlying database transformations, we break downstream experiences in Looker. For example:

 - We change the name of a database column without changing the corresponding `sql` field in our Looker view, leaving our users with a database error when using that field.
 - We make changes to LookML without remembering to check the Content Validator for errors, disrupting Dashboards and Looks that our users rely on

We wanted a single tool to perform these checks for us and establish a baseline performance expectation for our Looker instance. We believe in the power of continuous integration for analytics, and we built `spectacles` to enhance the business intelligence layer of analytics CI pipelines.

## Installation

`spectacles` is a Python module distributed on pypi and can be installed with pip.

```bash
pip install dbt
```

## Getting started

`spectacles` uses a Looker user's API credentials to connect to your Looker instance. 

You will need to know the `client_id` and `client_secret` for the user you want to use. If you are running `spectacles` locally, you can use your own user. If you are running `spectacles` in a continous integration environment, we recommend creating a user decicated to run `spectacles`. 

A user's API credentials are generated and visible from the [users page](https://docs.looker.com/admin-options/settings/users) of your Looker instance's Admin section. 

## Arguments

Every `spectacles` command requires a `client_id`, `client_secret` and `looker_base_url`. These can either be passed as arguments in the command line, a `.yml` file, or as environment variables.


## Testing your connection

Before you validate your LookML, you can test that `spectacles` is able to connect to your Looker instance.

```bash
spectacles connect
```

## SQL validator

The SQL validator

1. Connect to the API and establish a development mode session
2. Check out the CI build branch of interest on the project
3. Parse each Explore in the model for exposed dimensions
4. Query each Explore with all exposed dimensions to find any broken fields

The SQL validator is run with the `sql` command:

```bash
spectacles sql --project project_name --branch branch_name
```