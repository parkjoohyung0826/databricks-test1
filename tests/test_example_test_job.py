"""Unit tests for the example Lakeflow Job notebook."""

import importlib.util
from pathlib import Path
import unittest

NOTEBOOK_PATH = (
    Path(__file__).parents[1] / "src" / "notebooks" / "example_test_job.py"
)
SPEC = importlib.util.spec_from_file_location("example_test_job", NOTEBOOK_PATH)
assert SPEC is not None and SPEC.loader is not None
example_test_job = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(example_test_job)


class ExampleTestJobTests(unittest.TestCase):
    def test_build_result_normalizes_message(self) -> None:
        self.assertEqual(
            example_test_job.build_result("  hello databricks  "),
            {"status": "ok", "message": "hello databricks"},
        )

    def test_build_result_rejects_empty_message(self) -> None:
        with self.assertRaisesRegex(ValueError, "message must not be empty"):
            example_test_job.build_result("   ")


if __name__ == "__main__":
    unittest.main()
