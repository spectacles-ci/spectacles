import pytest
from spectacles.client import LookerClient
from spectacles.lookml import Explore, build_project, build_explore_fields
from spectacles.exceptions import SpectaclesException


class TestBuildProject:
    async def test_model_explore_field_counts_should_match(
        self, looker_client: LookerClient
    ):
        project = await build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_fields=True,
        )
        assert len(project.models) == 1
        assert len(project.models[0].explores) == 1
        fields = project.models[0].explores[0].fields
        assert len(fields) == 7
        assert "users.city" in [dim.name for dim in fields]
        assert not project.errored
        assert project.queried is False

    async def test_project_with_everything_excluded_should_not_have_models(
        self, looker_client: LookerClient
    ):
        project = await build_project(
            looker_client, name="eye_exam", filters=["-eye_exam/*"]
        )
        assert len(project.models) == 0

    async def test_duplicate_selectors_should_be_deduplicated(
        self, looker_client: LookerClient
    ):
        project = await build_project(
            looker_client, name="eye_exam", filters=["eye_exam/users", "eye_exam/users"]
        )
        assert len(project.models) == 1

    async def test_hidden_field_should_be_excluded_with_ignore_hidden(
        self, looker_client: LookerClient
    ):
        project = await build_project(
            looker_client,
            name="eye_exam",
            filters=["eye_exam/users"],
            include_fields=True,
            ignore_hidden_fields=True,
        )
        assert len(project.models[0].explores[0].fields) == 6


class TestBuildUnconfiguredProject:
    """Test for a build error when building an unconfigured LookML project."""

    async def test_project_with_no_configured_models_should_raise_error(
        self, looker_client: LookerClient
    ):
        await looker_client.update_workspace(workspace="production")
        with pytest.raises(SpectaclesException):
            await build_project(looker_client, name="eye_exam_unconfigured")


class TestBuildFields:
    async def test_field_count_should_match(
        self, looker_client: LookerClient, explore: Explore
    ):
        await build_explore_fields(looker_client, explore)
        assert len(explore.fields) == 7

    async def test_hidden_fields_should_be_excluded_with_ignore_hidden(
        self, looker_client: LookerClient, explore: Explore
    ):
        await build_explore_fields(
            looker_client,
            explore,
            ignore_hidden_fields=True,
        )
        assert len(explore.fields) == 6
