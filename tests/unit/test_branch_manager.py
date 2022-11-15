from unittest.mock import patch, AsyncMock, MagicMock
from spectacles.runner import LookerBranchManager
from spectacles.client import LookerClient

# Test the scenario where the user is testing Project A
# with these imports: A imports B & C; B imports C;
# Thus, C should not be imported twice
@patch.object(LookerBranchManager, "get_project_imports")
async def test_redundant_project_imports_are_skipped(
    get_project_imports: AsyncMock,
) -> None:
    # Mock calls for project A, then B, then C
    get_project_imports.side_effect = (["B", "C"], ["C"], [])
    mock_client = MagicMock(spec=LookerClient)
    manager = LookerBranchManager(mock_client, project="A")
    await manager(ref="dev-branch").__aenter__()

    # Project A should have only imported one project (B), skipping project C
    assert len(manager.import_managers) == 1
    assert manager.import_managers[0].project == "B"

    # Project B should contain the import for project C
    project_b_manager = manager.import_managers[0]
    assert len(project_b_manager.import_managers) == 1
    assert project_b_manager.import_managers[0].project == "C"
