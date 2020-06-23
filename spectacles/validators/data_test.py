from typing import Optional, Dict, Any
from spectacles.validators.validator import Validator
from spectacles.lookml import Explore
from spectacles.exceptions import SpectaclesException, DataTestError


class DataTestValidator(Validator):
    """Runs LookML/data tests for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def validate(self) -> Dict[str, Any]:
        all_tests = self.client.all_lookml_tests(self.project.name)

        # Filter the list of tests to those that are selected
        selected_tests = []
        # The error objects don't contain the name of the explore
        # We create this mapping to help look up the explore from the test name
        test_to_explore = {}

        for test in all_tests:
            model_name = test["model_name"]
            explore_name = test["explore_name"]
            explore: Optional[Explore] = self.project.get_explore(
                model=model_name, name=explore_name
            )

            # Skip tests that are not associated with a selected explore
            if explore is None:
                continue

            selected_tests.append(test)
            test_to_explore[test["name"]] = explore

        if len(selected_tests) == 0:
            raise SpectaclesException(
                name="no-data-tests-found",
                title="No data tests found.",
                detail=(
                    "If you're using --explores or --exclude, make sure your project "
                    "has data tests that reference those models or explores."
                ),
            )

        for test in selected_tests:
            results = self.client.run_lookml_test(
                self.project.name, model=test["model_name"], test=test["name"]
            )
            explore = test_to_explore[test["name"]]
            explore.queried = True
            result = results[0]  # For a single test, list with length 1

            for error in result["errors"]:
                project, file_path = error["file_path"].split("/", 1)
                lookml_url = (
                    f"{self.client.base_url}/projects/{self.project.name}"
                    f"/files/{file_path}?line={error['line_number']}"
                )
                explore.errors.append(
                    DataTestError(
                        model=error["model_id"],
                        explore=error["explore"],
                        message=error["message"],
                        test_name=result["test_name"],
                        lookml_url=lookml_url,
                    )
                )

        return self.project.get_results(validator="data_test")
