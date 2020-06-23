from spectacles.runner import Runner
from tests.utils import build_validation


def test_incremental_same_results_should_not_have_errors():
    main = build_validation("content")
    additional = build_validation("content")
    incremental = Runner._incremental_results(main, additional)
    assert incremental["status"] == "passed"
    assert incremental["errors"] == []
    assert incremental["tested"] == [
        dict(model="ecommerce", explore="orders", passed=True),
        dict(model="ecommerce", explore="sessions", passed=True),
        dict(model="ecommerce", explore="users", passed=True),
    ]


def test_incremental_with_fewer_errors_than_main():
    main = build_validation("content")
    additional = build_validation("content")
    additional["tested"][2]["passed"] = True
    additional["errors"] = []
    incremental = Runner._incremental_results(main, additional)
    assert incremental["status"] == "passed"
    assert incremental["errors"] == []
    assert incremental["tested"] == [
        dict(model="ecommerce", explore="orders", passed=True),
        dict(model="ecommerce", explore="sessions", passed=True),
        dict(model="ecommerce", explore="users", passed=True),
    ]


def test_incremental_with_more_errors_than_main():
    main = build_validation("content")
    additional = build_validation("content")
    additional["tested"][1]["passed"] = False
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
    additional["errors"].extend(extra_errors)
    incremental = Runner._incremental_results(main, additional)
    assert incremental["status"] == "failed"
    assert incremental["errors"] == extra_errors
    assert incremental["tested"] == [
        dict(model="ecommerce", explore="orders", passed=True),
        dict(model="ecommerce", explore="sessions", passed=False),
        dict(model="ecommerce", explore="users", passed=False),
    ]


def test_incremental_with_fewer_tested_explores_than_main():
    main = build_validation("content")
    additional = build_validation("content")
    _ = additional["tested"].pop(0)
    extra_error = dict(
        model="ecommerce",
        explore="users",
        test=None,
        message="Another error occurred",
        metadata={},
    )
    additional["errors"].append(extra_error)
    incremental = Runner._incremental_results(main, additional)
    assert incremental["status"] == "failed"
    assert incremental["errors"] == [extra_error]
    assert incremental["tested"] == [
        dict(model="ecommerce", explore="sessions", passed=True),
        dict(model="ecommerce", explore="users", passed=False),
    ]
