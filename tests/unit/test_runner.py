from unittest.mock import patch, Mock, MagicMock, AsyncMock
import jsonschema
from spectacles.lookml import Project, Model, Explore
from spectacles.types import JsonDict
from spectacles.client import LookerClient
from spectacles.exceptions import ContentError
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
):
    error_message = "An error ocurred"

    def add_error_to_project(tests):
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


def test_incremental_same_results_should_not_have_errors():
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


def test_incremental_with_fewer_errors_than_target():
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


def test_incremental_with_more_errors_than_target():
    base = build_validation("content")
    target = build_validation("content")
    base["tested"][1]["status"] = "failed"
    extra_errors = [
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


def test_incremental_with_fewer_tested_explores_than_target():
    base = build_validation("content")
    target = build_validation("content")
    _ = base["tested"].pop(0)
    extra_error = dict(
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
