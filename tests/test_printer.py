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
