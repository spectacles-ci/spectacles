import pytest
from spectacles.client import LookerClient
from spectacles.lookml import Explore, build_project, build_explore_dimensions
from spectacles.exceptions import SpectaclesException


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildProject:
    async def test_model_explore_dimension_counts_should_match(self, looker_client):
        project = await build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_dimensions=True,
        )
        assert len(project.models) == 1
        assert len(project.models[0].explores) == 1
        dimensions = project.models[0].explores[0].dimensions
        assert len(dimensions) == 6
        assert "users.city" in [dim.name for dim in dimensions]
        assert not project.errored
        assert project.queried is False

    async def test_project_with_everything_excluded_should_not_have_models(
        self, looker_client
    ):
        project = await build_project(
            looker_client, name="eye_exam", filters=["-eye_exam/*"]
        )
        assert len(project.models) == 0

    async def test_duplicate_selectors_should_be_deduplicated(self, looker_client):
        project = await build_project(
            looker_client, name="eye_exam", filters=["eye_exam/users", "eye_exam/users"]
        )
        assert len(project.models) == 1

    async def test_hidden_dimension_should_be_excluded_with_ignore_hidden(
        self, looker_client
    ):
        project = await build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_dimensions=True,
            ignore_hidden_fields=True,
        )
        assert len(project.models[0].explores[0].dimensions) == 5


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildUnconfiguredProject:
    """Test for a build error when building an unconfigured LookML project."""

    async def test_project_with_no_configured_models_should_raise_error(
        self, looker_client
    ):
        await looker_client.update_workspace(workspace="production")
        with pytest.raises(SpectaclesException):
            await build_project(looker_client, name="eye_exam_unconfigured")


@pytest.mark.vcr(match_on=["uri", "method", "raw_body"])
class TestBuildDimensions:
    async def test_dimension_count_should_match(
        self, looker_client: LookerClient, explore: Explore
    ):
        await build_explore_dimensions(looker_client, explore)
        assert len(explore.dimensions) == 6

    async def test_hidden_dimension_should_be_excluded_with_ignore_hidden(
        self, looker_client: LookerClient, explore: Explore
    ):
        await build_explore_dimensions(
            looker_client,
            explore,
            ignore_hidden_fields=True,
        )
        assert len(explore.dimensions) == 5
