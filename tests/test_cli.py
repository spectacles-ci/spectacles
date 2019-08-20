import os
from unittest.mock import patch, Mock
from unittest import TestCase
import yaml
import pytest
import click
from click.testing import CliRunner
from tests.constants import TEST_BASE_URL, ENV_VARS
from fonz.cli import create_parser
import logging


@pytest.fixture
def clean_env(monkeypatch):
    for variable in ENV_VARS.keys():
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def env(monkeypatch):
    for variable, value in ENV_VARS.items():
        monkeypatch.setenv(variable, value)


@pytest.fixture
def limited_env(monkeypatch):
    for variable, value in ENV_VARS.items():
        if variable in ["LOOKER_CLIENT_SECRET", "LOOKER_PROJECT"]:
            monkeypatch.delenv(variable, raising=False)
        else:
            monkeypatch.setenv(variable, value)


@pytest.fixture()
def parser():
    parser = create_parser()
    return parser


def test_help(parser):
    with pytest.raises(SystemExit) as cm:
        parsed = parser.parse_args(["--help"])


def test_something(parser):
    parsed = parser.parse_args(["connect", "--base-url", "url"])
    assert parsed.base_url == "url"


def test_something_else(env, parser):
    parsed = parser.parse_args(["connect", "--base-url", "url"])
    assert parsed.base_url == "url"
    assert parsed.client_secret == "CLIENT_SECRET_ENV_VAR"
