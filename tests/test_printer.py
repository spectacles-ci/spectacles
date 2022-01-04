from unittest.mock import patch
from spectacles import printer
from spectacles.logger import delete_color_codes


def test_extract_sql_context():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 4\n| 5\n* 6\n| 7\n| 8"
    result = printer.extract_sql_context(sql=text, line_number=6, window_size=2)
    assert delete_color_codes(result) == expected_result


def test_extract_sql_context_line_number_close_to_end():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 17\n| 18\n* 19\n| 20"
    result = printer.extract_sql_context(sql=text, line_number=19, window_size=2)
    assert delete_color_codes(result) == expected_result


def test_extract_sql_context_line_number_close_to_beginning():
    text = "\n".join([f"{n}" for n in range(1, 21)])
    expected_result = "| 1\n* 2\n| 3\n| 4"
    result = printer.extract_sql_context(sql=text, line_number=2, window_size=2)
    assert delete_color_codes(result) == expected_result


def test_mark_line_odd_number_of_lines():
    text = [f"{n}" for n in range(1, 6)]
    expected_result = ["| 1", "| 2", "* 3", "| 4", "| 5"]
    result = printer.mark_line(lines=text, line_number=3)
    result = [delete_color_codes(line) for line in result]
    assert result == expected_result


def test_mark_line_even_number_of_lines():
    text = [f"{n}" for n in range(1, 5)]
    expected_result = ["| 1", "* 2", "| 3", "| 4"]
    result = printer.mark_line(lines=text, line_number=2)
    result = [delete_color_codes(line) for line in result]
    assert result == expected_result


@patch("spectacles.printer.log_sql_error", return_value="path_to_sql_file")
def test_sql_error_prints_with_relevant_info(mock_log, sql_error, caplog):
    model = "model_a"
    explore = "explore_a"
    dimension = "view_a.dimension_a"
    message = "A super important error occurred."

    printer.print_sql_error(
        model=model,
        explore=explore,
        message=message,
        sql="SELECT * FROM test",
        log_dir="logs",
    )
    assert "LookML:" not in caplog.text
    assert model in caplog.text
    assert explore in caplog.text
    assert message in caplog.text
    assert dimension not in caplog.text

    printer.print_sql_error(
        model=model,
        explore=explore,
        message=message,
        sql="SELECT * FROM test",
        log_dir=None,
        lookml_url="https://spectacles.looker.com",
    )
    assert "LookML:" in caplog.text


def test_content_error_prints_with_relevant_info(sql_error, caplog):
    model = "model_a"
    explore = "explore_a"
    content_type = "dashboard"
    tile_type = "dashboard_filter"
    tile_title = "That one filter"
    space = "Shared"
    message = "A super important error occurred."
    title = "My Dashboard"

    printer.print_content_error(
        model=model,
        explore=explore,
        message=message,
        content_type=content_type,
        tile_type=tile_type,
        tile_title=tile_title,
        space=space,
        title=title,
        url="https://spectacles.looker.com",
    )
    assert model in caplog.text
    assert explore in caplog.text
    assert content_type.title() in caplog.text
    assert "Filter" in caplog.text
    assert tile_title in caplog.text
    assert message in caplog.text
    assert space in caplog.text
    assert title in caplog.text


def test_data_test_error_prints_with_relevant_info(sql_error, caplog):
    model = "model_a"
    explore = "explore_a"
    test_name = "assert_metric_is_positive"
    message = "A super important error occurred."
    lookml_url = "https://spectacles.looker.com"

    printer.print_data_test_error(
        model=model,
        explore=explore,
        test_name=test_name,
        message=message,
        lookml_url=lookml_url,
    )
    assert model in caplog.text
    assert explore in caplog.text
    assert test_name in caplog.text
    assert message in caplog.text
    assert lookml_url in caplog.text


def test_print_validation_result_should_work():
    printer.print_validation_result(passed=True, source="model.explore")
    printer.print_validation_result(passed=False, source="model.explore")
