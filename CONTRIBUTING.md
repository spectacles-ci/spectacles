# Contributing to Spectacles
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

## VCR testing in spectacles

Some of the tests in Spectacles use [VCR.py](https://vcrpy.readthedocs.io/en/latest/), a library for testing external HTTP requests and [pytest-recording](https://github.com/kiwicom/pytest-recording), a pytest plugin for vcrpy. Here's how it works:

>VCR.py simplifies and speeds up tests that make HTTP requests. The first time you run code that is inside a VCR.py context manager or decorated function, VCR.py records all HTTP interactions that take place through the libraries it supports and serializes and writes them to a flat file (in yaml format by default). This flat file is called a cassette. When the relevant piece of code is executed again, VCR.py will read the serialized requests and responses from the aforementioned cassette file, and intercept any HTTP requests that it recognizes from the original test run and return the responses that corresponded to those requests. This means that the requests will not actually result in HTTP traffic.

>If the server you are testing against ever changes its API, all you need to do is delete your existing cassette files, and run your tests again. VCR.py will detect the absence of a cassette file and once again record all HTTP interactions, which will update them to correspond to the new API.

When developing new Spectacles tests, we can record requests and responses to cassettes and commit them. In CI, we run without the option to make new requests and only use the committed, serialized (YAML) responses in the cassettes. This means tests will be nice and speedy (sometimes up to 10x faster) and won't make external requests.

**We can configure the mode VCR.py runs in with the `--record-mode` option to pytest. Unless you are developing new tests that make external HTTP requests, you should use `--record-mode none`, which will only use the existing pre-recorded cassettes.**

If you want to add new tests that make VCR requests, here's a summary of the steps you should take:

1. Export your API credentials in environment variables
1. Write tests or fixtures that make new API calls (see below for some caveats)
1. Run pytest with `--record-mode new_episodes` to populate or modify cassettes
1. You may need to retry the previous step after deleting your cassettes folder if there are conflicts
1. Try unsetting your credentials environment variables and running pytest with `--record-mode none`
1. If all tests pass, commit any additions or changes to the cassettes

You'll also want to be aware of some dos and don'ts for working with VCR.py in our test setup:

### Do set your API credentials in environment variables

We've set up the shared `looker_client` fixture in `conftest.py` to authenticate using the environment variables `LOOKER_CLIENT_ID` and `LOOKER_CLIENT_SECRET`. You'll need to set those environment variables in order for tests to run when you're not playing from cassettes.

Then you can use that fixture or any fixtures that use it to make requests to Looker.

### Don't commit sensitive data in cassettes
All requests and responses that are recorded are saved to YAML cassettes in `tests/cassettes`. This means that it's possible to commit sensitive data like Looker client secret or an access token (less sensitive because they expire in one hour).

You should install our pre-commit hooks, one of which will check for tokens in cassettes before you commit.

```bash
pre-commit install
```

These checks will run before every commit.

By default, any test marked with `pytest.mark.vcr` will have `Authorization` headers filtered out from the cassettes.

Here's an example of how you might need to do some additional filtering. In this example, we use two arguments to `pytest.mark.vcr`. We specify `filter_post_data_parameters` (filters POST request params to remove the client ID and secret) and `before_record_response` (removes the access token from the response).

```python
def scrub_access_token(response):
    body = json.loads(response["body"]["string"].decode())
    body["access_token"] = ""
    response["body"]["string"] = json.dumps(body).encode()
    return response

@pytest.mark.default_cassette("init_client.yaml")
@pytest.mark.vcr(
    filter_post_data_parameters=["client_id", "client_secret"],
    before_record_response=scrub_access_token,
)
@pytest.fixture(scope="session")
def looker_client(record_mode) -> Iterable[LookerClient]:
    client = LookerClient(
        base_url="https://spectacles.looker.com",
        client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
        client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
    )
    yield client
```

For other options, the documentation for VCR.py has some [good examples](https://vcrpy.readthedocs.io/en/latest/advanced.html#filter-sensitive-data-from-the-request).

### Do refresh your cassettes before pushing

When developing new tests and cassettes, it's good practice to delete the cassettes directory and refresh it with new calls (specifying `new_episodes` for record mode so requests are re-recorded). Here's an example:

```
rm -r tests/cassettes && pytest tests --record-mode new_episodes
```

### Do test in `--record-mode none` before pushing

Once you've developed some tests and recorded them to cassetes (by running with `--record-mode new_episodes`), you'll want to confirm that tests can run offline and independently of API credentials. If your tests can't run that way, they will fail in CI. To confirm your tests work offline, unset your environment variables and run pytest with `--record-mode none`.

### Do be aware of default matching behavior
When VCR.py detects a request, it checks to see if there is a matching request in the designated request that it can use instead. By default, it matches requests on URL and method (GET, POST, etc.). For many API requests, this is not sufficient, because they are differentiated by JSON body.

For most of the tests in Spectacles, we provide `match_on` to `pytest.mark.vcr` and specify `raw_body` as an additional matching parameter so requests are only matched if they also have the same body.
