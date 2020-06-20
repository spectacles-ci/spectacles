from typing import Optional, List, Dict, Any
from collections import OrderedDict
from spectacles.client import LookerClient
from spectacles.validators.validator import Validator
from spectacles.select import is_selected
from spectacles.exceptions import SpectaclesException, DataTestError
import spectacles.printer as printer


class DataTestValidator(Validator):
    """Runs LookML/data tests for a given project.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    """

    def __init__(self, client: LookerClient, project: str):
        super().__init__(client)
        self.project = project

    def validate(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        # Assign default values for selectors and exclusions
        if selectors is None:
            selectors = ["*/*"]
        if exclusions is None:
            exclusions = []

        all_tests = self.client.all_lookml_tests(self.project)
        selected_tests = []
        test_to_explore = {}
        for test in all_tests:
            if is_selected(
                test["model_name"], test["explore_name"], selectors, exclusions
            ):
                selected_tests.append(test)
                # The error objects don't contain the name of the explore
                # We create this mapping to help look up the explore from the test name
                test_to_explore[test["name"]] = test["explore_name"]

        test_count = len(selected_tests)
        if test_count == 0:
            raise SpectaclesException(
                name="no-data-tests-found",
                title="No data tests found.",
                detail=(
                    "If you're using --explores or --exclude, make sure your project "
                    "has data tests that reference those models or explores."
                ),
            )

        printer.print_header(
            f"Running {test_count} {'test' if test_count == 1 else 'tests'}"
        )

        test_results: List[Dict[str, Any]] = []
        for test in selected_tests:
            test_name = test["name"]
            model_name = test["model_name"]
            results = self.client.run_lookml_test(
                self.project, model=model_name, test=test_name
            )
            test_results.extend(results)

        tested = []
        errors = []

        for result in test_results:
            explore = test_to_explore[result["test_name"]]
            test = {
                "model": result["model_name"],
                "explore": explore,
                "passed": result["success"],
            }
            tested.append(test)
            if not result["success"]:
                for error in result["errors"]:
                    project, file_path = error["file_path"].split("/", 1)
                    lookml_url = (
                        f"{self.client.base_url}/projects/{self.project}"
                        f"/files/{file_path}?line={error['line_number']}"
                    )
                    errors.append(
                        DataTestError(
                            model=error["model_id"],
                            explore=error["explore"],
                            message=error["message"],
                            test_name=result["test_name"],
                            lookml_url=lookml_url,
                        ).__dict__
                    )

        def reduce_result(results):
            """Aggregate individual test results to get pass/fail by explore"""
            agg = OrderedDict()
            for result in results:
                # Keys by model and explore, adds additional values for `passed` to a set
                agg.setdefault((result["model"], result["explore"]), set()).add(
                    result["passed"]
                )
            reduced = [
                {"model": k[0], "explore": k[1], "passed": min(v)}
                for k, v in agg.items()
            ]
            return reduced

        tested = reduce_result(tested)
        for test in tested:
            printer.print_validation_result(
                passed=test["passed"], source=f"{test['model']}.{test['explore']}"
            )

        passed = min((test["passed"] for test in tested), default=True)
        return {
            "validator": "data_test",
            "status": "passed" if passed else "failed",
            "tested": tested,
            "errors": errors,
        }
