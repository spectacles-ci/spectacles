from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import jsonschema

from spectacles.client import LookerClient
from spectacles.exceptions import ContentError
from spectacles.lookml import Explore, Model, Project
from spectacles.models import JsonDict
from spectacles.runner import Runner
from tests.utils import build_validation


@patch("spectacles.validators.content.ContentValidator.validate")
@patch("spectacles.runner.build_project")
@patch("spectacles.runner.LookerBranchManager")
async def test_validate_content_returns_valid_schema(
    mock_branch_manager: MagicMock,
    mock_build_project: AsyncMock,
    mock_validate: AsyncMock,
    project: Project,
    model: Model,
    explore: Explore,
    schema: JsonDict,
) -> None:
    error_message = "An error ocurred"

    def add_error_to_project(_: Any) -> None:
        project.models[0].explores[0].queried = True
        project.models[0].explores[0].errors = [
            ContentError("", "", error_message, "", "", "", "", "")
        ]

    model.explores = [explore]
    project.models = [model]
    mock_build_project.return_value = project
    mock_validate.side_effect = add_error_to_project
    runner = Runner(client=Mock(spec=LookerClient), project="eye_exam")
    result = await runner.validate_content()
    assert result["status"] == "failed"
    assert result["errors"][0]["message"] == error_message
    jsonschema.validate(result, schema)


def test_incremental_same_results_should_not_have_errors() -> None:
    base = build_validation("content")
    target = build_validation("content")
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "passed"
    assert diff["errors"] == []
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="passed"),
    ]


def test_incremental_with_fewer_errors_than_target() -> None:
    base = build_validation("content")
    target = build_validation("content")
    base["tested"][2]["status"] = "passed"
    base["errors"] = []
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "passed"
    assert diff["errors"] == []
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="passed"),
    ]


def test_incremental_with_more_errors_than_target() -> None:
    base = build_validation("content")
    target = build_validation("content")
    base["tested"][1]["status"] = "failed"
    extra_errors: List[Dict[Any, Any]] = [
        dict(
            model="ecommerce",
            explore="users",
            test=None,
            message="Another error occurred",
            metadata={},
        ),
        dict(
            model="ecommerce",
            explore="sessions",
            test=None,
            message="An error occurred",
            metadata={},
        ),
    ]
    base["errors"].extend(extra_errors)
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "failed"
    assert diff["errors"] == extra_errors
    assert diff["tested"] == [
        dict(model="ecommerce", explore="orders", status="passed"),
        dict(model="ecommerce", explore="sessions", status="failed"),
        dict(model="ecommerce", explore="users", status="failed"),
    ]


def test_incremental_with_fewer_tested_explores_than_target() -> None:
    base = build_validation("content")
    target = build_validation("content")
    _ = base["tested"].pop(0)
    extra_error: dict[Any, Any] = dict(
        model="ecommerce",
        explore="users",
        test=None,
        message="Another error occurred",
        metadata={},
    )
    base["errors"].append(extra_error)
    diff = Runner._incremental_results(base, target)
    assert diff["status"] == "failed"
    assert diff["errors"] == [extra_error]
    assert diff["tested"] == [
        dict(model="ecommerce", explore="sessions", status="passed"),
        dict(model="ecommerce", explore="users", status="failed"),
    ]
