import time
from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod
from collections import OrderedDict
from spectacles.client import LookerClient
from spectacles.lookml import Project, Model, Explore, Dimension
from spectacles.types import QueryMode
from spectacles.select import is_selected
from spectacles.logger import GLOBAL_LOGGER as logger
from spectacles.exceptions import (
    SqlError,
    DataTestError,
    ContentError,
    SpectaclesException,
    LookMlNotFound,
)
import spectacles.printer as printer


class Query:
    """Stores IDs and a reference to the LookML object being queried"""

    def __init__(
        self,
        query_id: str,
        lookml_ref: Union[Dimension, Explore],
        explore_url: str,
        query_task_id: Optional[str] = None,
    ):
        self.query_id = query_id
        self.lookml_ref = lookml_ref
        self.explore_url = explore_url
        self.query_task_id = query_task_id


class QueryResult:
    """Stores ID, query status, and error details for a completed query task"""

    def __init__(
        self, query_task_id: str, status: str, error: Optional[Dict[str, Any]] = None
    ):
        self.query_task_id = query_task_id
        self.status = status
        self.error = error


class Validator(ABC):  # pragma: no cover
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    @abstractmethod
    def validate(self):
        raise NotImplementedError


class ContentValidator(Validator):
    def __init__(
        self, client: LookerClient, project: str, exclude_personal: bool = False
    ):
        super().__init__(client)
        self.project = Project(project, models=[])
        personal_folders = self.get_personal_folders() if exclude_personal else []
        self.personal_folders: List[int] = personal_folders

    def get_personal_folders(self) -> List[int]:
        personal_folders = []
        result = self.client.all_folders(self.project.name)
        for folder in result:
            if folder["is_personal"] or folder["is_personal_descendant"]:
                personal_folders.append(folder["id"])
        return personal_folders

    def build_project(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
    ) -> None:
        """Creates an object representation of the project's LookML.

        Args:
            selectors: List of selector strings in 'model_name/explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name/*' would select all explores in the 'model_name' model.

        """
        # Assign default values for selectors and exclusions
        if selectors is None:
            selectors = ["*/*"]
        if exclusions is None:
            exclusions = []

        logger.info(
            f"Building LookML project hierarchy for project {self.project.name}"
        )

        all_models = [
            Model.from_json(model) for model in self.client.get_lookml_models()
        ]
        project_models = [
            model for model in all_models if model.project_name == self.project.name
        ]

        if not project_models:
            raise LookMlNotFound(
                name="project-models-not-found",
                title="No configured models found for the specified project.",
                detail=(
                    f"Go to {self.client.base_url}/projects and confirm "
                    "a) at least one model exists for the project and "
                    "b) it has an active configuration."
                ),
            )

        for model in project_models:
            model.explores = [
                explore
                for explore in model.explores
                if is_selected(model.name, explore.name, selectors, exclusions)
            ]

        self.project.models = [
            model for model in project_models if len(model.explores) > 0
        ]

    def validate(self):
        errors = []
        result = self.client.content_validation()

        for content in result["content_with_errors"]:
            try:
                content_type = self.get_content_type(content)
            except KeyError:
                logger.debug(
                    f"Skipping content because it does not seem to be a dashboard or "
                    f"a look. The content received was: {content}"
                )
                continue

            # If exclude_personal isn't specified, personal_folders list is empty
            if content[content_type]["folder"]["id"] in self.personal_folders:
                continue
            else:
                content_errors = self.errors_from_result(content, content_type)
                errors.extend(content_errors)

        unique_errors = []
        for error in errors:
            if error.__dict__ not in unique_errors:
                unique_errors.append(error.__dict__)

        # TODO: Get information on all content so we can return a useful "tested"
        return {
            "validator": "content",
            "status": "failed" if unique_errors else "passed",
            "tested": [],
            "errors": unique_errors,
        }

    @staticmethod
    def get_content_type(content: Dict[str, Any]) -> str:
        if content["dashboard"]:
            return "dashboard"
        elif content["look"]:
            return "look"
        else:
            raise KeyError("Content type not found. Valid keys are 'look', 'dashboard'")

    def is_project_member(self, model: str, explore: str) -> bool:
        matching_model = next((m for m in self.project.models if m.name == model), None)
        if matching_model is None:
            return False
        if next((True for e in matching_model.explores if e.name == explore), False):
            return True
        else:
            return False

    def errors_from_result(
        self, content: Dict, content_type: str
    ) -> List[ContentError]:
        errors = []
        for error in content["errors"]:
            model_name = error["model_name"]
            explore_name = error["explore_name"]
            if not self.is_project_member(model_name, explore_name):
                continue

            content_id = content[content_type]["id"]
            errors.append(
                ContentError(
                    model=model_name,
                    explore=explore_name,
                    message=error["message"],
                    field_name=error["field_name"],
                    content_type=content_type,
                    title=content[content_type]["title"],
                    space=content[content_type]["space"]["name"],
                    url=f"{self.client.base_url}/{content_type}s/{content_id}",
                )
            )
        return errors


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


class SqlValidator(Validator):
    """Runs and validates the SQL for each selected LookML dimension.

    Args:
        client: Looker API client.
        project: Name of the LookML project to validate.

    Attributes:
        project: LookML project object representation.
        query_tasks: Mapping of query task IDs to LookML objects

    """

    def __init__(self, client: LookerClient, project: str, concurrency: int = 10):
        super().__init__(client)

        self.project = Project(project, models=[])
        self.query_slots = concurrency
        self._running_queries: List[Query] = []
        # Lookup used to retrieve the LookML object
        self._query_by_task_id: Dict[str, Query] = {}

    def get_query_by_task_id(self, query_task_id: str) -> Query:
        return self._query_by_task_id[query_task_id]

    def get_running_query_tasks(self) -> List[str]:
        return [
            query.query_task_id
            for query in self._running_queries
            if query.query_task_id
        ]

    def build_project(
        self,
        selectors: Optional[List[str]] = None,
        exclusions: Optional[List[str]] = None,
    ) -> None:
        """Creates an object representation of the project's LookML.

        Args:
            selectors: List of selector strings in 'model_name/explore_name' format.
                The '*' wildcard selects all models or explores. For instance,
                'model_name/*' would select all explores in the 'model_name' model.

        """
        # Assign default values for selectors and exclusions
        if selectors is None:
            selectors = ["*/*"]
        if exclusions is None:
            exclusions = []

        logger.info(
            f"Building LookML project hierarchy for project {self.project.name}"
        )

        all_models = [
            Model.from_json(model) for model in self.client.get_lookml_models()
        ]
        project_models = [
            model for model in all_models if model.project_name == self.project.name
        ]

        if not project_models:
            raise LookMlNotFound(
                name="project-models-not-found",
                title="No configured models found for the specified project.",
                detail=(
                    f"Go to {self.client.base_url}/projects and confirm "
                    "a) at least one model exists for the project and "
                    "b) it has an active configuration."
                ),
            )

        for model in project_models:
            model.explores = [
                explore
                for explore in model.explores
                if is_selected(model.name, explore.name, selectors, exclusions)
            ]
            for explore in model.explores:
                dimensions_json = self.client.get_lookml_dimensions(
                    model.name, explore.name
                )
                for dimension_json in dimensions_json:
                    dimension = Dimension.from_json(
                        dimension_json, model.name, explore.name
                    )
                    dimension.url = self.client.base_url + dimension.url
                    if not dimension.ignore:
                        explore.add_dimension(dimension)

        self.project.models = [
            model for model in project_models if len(model.explores) > 0
        ]

    def validate(self, mode: QueryMode = "batch") -> Dict[str, Any]:
        """Queries selected explores and returns the project tree with errors."""
        self._query_by_task_id = {}

        self._create_and_run(mode)
        if mode == "hybrid" and self.project.errored:
            self._create_and_run(mode)

        return self.project.get_results(validator="sql", mode=mode)

    def _create_and_run(self, mode: QueryMode = "batch") -> None:
        """Runs a single validation using a specified mode"""
        queries: List[Query] = []
        try:
            queries = self._create_queries(mode)
            self._run_queries(queries)
        except KeyboardInterrupt:
            logger.info(
                "\n\n" + "Please wait, asking Looker to cancel any running queries..."
            )
            query_tasks = self.get_running_query_tasks()
            self._cancel_queries(query_tasks)
            if query_tasks:
                message = (
                    f"Attempted to cancel {len(query_tasks)} running "
                    f"{'query' if len(query_tasks) == 1 else 'queries'}."
                )
            else:
                message = (
                    "No queries were running at the time so nothing was cancelled."
                )
            raise SpectaclesException(
                name="validation-keyboard-interrupt",
                title="SQL validation was manually interrupted.",
                detail=message,
            )

    def _create_queries(self, mode: QueryMode) -> List[Query]:
        """Creates a list of queries to be executed for validation"""
        queries: List[Query] = []
        for model in self.project.models:
            for explore in model.explores:
                if mode == "batch" or (mode == "hybrid" and not explore.queried):
                    query = self._create_explore_query(explore, model.name)
                    queries.append(query)
                elif mode == "single" or (mode == "hybrid" and explore.errored):
                    explore_queries = self._create_dimension_queries(
                        explore, model.name
                    )
                    queries.extend(explore_queries)
        return queries

    def _create_explore_query(self, explore: Explore, model_name: str) -> Query:
        """Creates a single query with all dimensions of an explore"""
        dimensions = [dimension.name for dimension in explore.dimensions]
        query = self.client.create_query(model_name, explore.name, dimensions)
        return Query(query["id"], lookml_ref=explore, explore_url=query["share_url"])

    def _create_dimension_queries(
        self, explore: Explore, model_name: str
    ) -> List[Query]:
        """Creates individual queries for each dimension in an explore"""
        queries = []
        for dimension in explore.dimensions:
            query = self.client.create_query(model_name, explore.name, [dimension.name])
            query = Query(
                query["id"], lookml_ref=dimension, explore_url=query["share_url"]
            )
            queries.append(query)
        return queries

    def _run_queries(self, queries: List[Query]) -> None:
        """Creates and runs queries with a maximum concurrency defined by query slots"""
        QUERY_TASK_LIMIT = 250

        while queries or self._running_queries:
            if queries:
                logger.debug(f"Starting a new loop, {len(queries)} queries queued")
                self._fill_query_slots(queries)
            query_tasks = self.get_running_query_tasks()[:QUERY_TASK_LIMIT]
            logger.debug(f"Checking for results of {len(query_tasks)} query tasks")
            for query_result in self._get_query_results(query_tasks):
                self._handle_query_result(query_result)
            time.sleep(0.5)

    def _fill_query_slots(self, queries: List[Query]) -> None:
        """Creates query tasks until all slots are used or all queries are running"""
        while queries and self.query_slots > 0:
            logger.debug(
                f"{self.query_slots} available query slots, creating query task"
            )
            query = queries.pop(0)
            query_task_id = self.client.create_query_task(query.query_id)
            self.query_slots -= 1
            query.query_task_id = query_task_id
            self._query_by_task_id[query_task_id] = query
            self._running_queries.append(query)

    def _get_query_results(self, query_task_ids: List[str]) -> List[QueryResult]:
        """Returns ID, status, and error message for all query tasks"""
        query_results = []
        results = self.client.get_query_task_multi_results(query_task_ids)
        for query_task_id, result in results.items():
            status = result["status"]
            if status not in ("complete", "error", "running", "added", "expired"):
                raise SpectaclesException(
                    name="unexpected-query-result-status",
                    title="Encountered an unexpected query result status.",
                    detail=(
                        f"Query result status '{status}' was returned "
                        "by the Looker API."
                    ),
                )
            logger.debug(f"Query task {query_task_id} status is: {status}")
            query_result = QueryResult(query_task_id, status)
            if status == "error":
                try:
                    error_details = self._extract_error_details(result)
                except (KeyError, TypeError, IndexError) as error:
                    logger.debug(
                        f"Exiting because of unexpected query result format: {result}"
                    )
                    raise SpectaclesException(
                        name="unexpected-query-result-format",
                        title="Encountered an unexpected query result format.",
                        detail=f"Unable to extract error details. The unexpected result has been logged.",
                    ) from error
                else:
                    query_result.error = error_details
            query_results.append(query_result)
        return query_results

    def _handle_query_result(self, result: QueryResult) -> Optional[SqlError]:
        query = self.get_query_by_task_id(result.query_task_id)
        if result.status in ("complete", "error"):
            self._running_queries.remove(query)
            self.query_slots += 1
            lookml_object = query.lookml_ref
            lookml_object.queried = True

            if result.status == "error" and result.error:
                model_name = lookml_object.model_name
                dimension_name: Optional[str] = None
                if isinstance(lookml_object, Dimension):
                    explore_name = lookml_object.explore_name
                    dimension_name = lookml_object.name
                else:
                    explore_name = lookml_object.name

                sql_error = SqlError(
                    model=model_name,
                    explore=explore_name,
                    dimension=dimension_name,
                    explore_url=query.explore_url,
                    lookml_url=getattr(lookml_object, "url", None),
                    **result.error,
                )
                lookml_object.errors.append(sql_error)
                return sql_error
        return None

    @staticmethod
    def _extract_error_details(query_result: Dict) -> Optional[Dict]:
        """Extracts the relevant error fields from a Looker API response"""
        data = query_result["data"]
        if isinstance(data, dict):
            errors = data.get("errors") or [data.get("error")]
            try:
                first_error = next(
                    error
                    for error in errors
                    if error.get("message")
                    != "Note: This query contains derived tables with conditional SQL for Development Mode. "
                    "Query results in Production Mode might be different."
                )
            except StopIteration:
                return None
            message = " ".join(
                filter(
                    None,
                    [first_error.get("message"), first_error.get("message_details")],
                )
            )
            sql = data.get("sql")
            error_loc = first_error.get("sql_error_loc")
            if error_loc:
                line_number = error_loc.get("line")
            else:
                line_number = None
        elif isinstance(data, list):
            message = data[0]
            line_number = None
            sql = None
        else:
            raise TypeError(
                "Unexpected error response type. "
                "Expected a dict or a list, "
                f"received type {type(data)}: {data}"
            )

        return {"message": message, "sql": sql, "line_number": line_number}

    def _cancel_queries(self, query_task_ids: List[str]) -> None:
        """Asks the Looker API to cancel specified queries"""
        for query_task_id in query_task_ids:
            self.client.cancel_query_task(query_task_id)
