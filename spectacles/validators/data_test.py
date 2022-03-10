from dataclasses import dataclass
from typing import List, Optional
from spectacles.client import LookerClient
from spectacles.lookml import Explore, Project
from spectacles.exceptions import SpectaclesException, DataTestError


@dataclass
class DataTest:
    name: str
    explore: Explore
    project_name: str
    base_url: str
    query_url_params: str
    file: str
    line: int
    passed: Optional[bool] = None

    def __post_init__(self):
        try:
            self.file_path = self.file.split("/", 1)[1]
        except IndexError:
            raise SpectaclesException(
                name="data-test-has-incorrect-file-path-format",
                title="A data test does not have the correct file path format.",
                detail=f"Couldn't extract file path from unexpected file '{self.file}'",
            )

    @property
    def explore_url(self):
        return (
            f"{self.base_url}/explore/{self.explore.model_name}"
            f"/{self.explore.name}?{self.query_url_params}"
        )

    @property
    def lookml_url(self) -> str:
        return (
            f"{self.base_url}/projects/{self.project_name}"
            f"/files/{self.file_path}?line={self.line}"
        )


class DataTestValidator:
    """Runs LookML/data tests for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    def get_tests(self, project: Project) -> List[DataTest]:
        all_tests = self.client.all_lookml_tests(project.name)

        # Filter the list of tests to those that are selected
        selected_tests: List[DataTest] = []

        for result in all_tests:
            explore = project.get_explore(
                model=result["model_name"], name=result["explore_name"]
            )

            # Skip tests that are not associated with a selected explore
            if explore:
                test = DataTest(
                    name=result["name"],
                    explore=explore,
                    project_name=project.name,
                    base_url=self.client.base_url,
                    query_url_params=result["query_url_params"],
                    file=result["file"],
                    line=result["line"],
                )

                selected_tests.append(test)

        if len(selected_tests) == 0:
            raise SpectaclesException(
                name="no-data-tests-found",
                title="No data tests found.",
                detail=(
                    "If you're using --explores, make sure your project "
                    "has data tests that reference those models or explores."
                ),
            )

        return selected_tests

    def validate(self, tests: List[DataTest]) -> List[DataTestError]:
        data_test_errors: List[DataTestError] = []
        for test in tests:
            results = self.client.run_lookml_test(
                test.project_name, model=test.explore.model_name, test=test.name
            )
            test.explore.queried = True
            result = results[0]  # For a single test, list with length 1

            if result["success"]:
                test.passed = True
                test.explore.successes.append(
                    {
                        "model": test.explore.model_name,
                        "explore": test.explore.name,
                        "metadata": {
                            "test_name": result["test_name"],
                            "lookml_url": test.lookml_url,
                            "explore_url": test.explore_url,
                        },
                    }
                )
            else:
                test.passed = False
                for error in result["errors"]:
                    error = DataTestError(
                        model=error["model_id"],
                        explore=error["explore"],
                        message=error["message"],
                        test_name=result["test_name"],
                        lookml_url=test.lookml_url,
                        explore_url=test.explore_url,
                    )
                    data_test_errors.append(error)
                    test.explore.errors.append(error)

        return data_test_errors
