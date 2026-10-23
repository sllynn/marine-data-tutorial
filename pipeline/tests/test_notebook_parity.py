"""Ensure the package cannot silently run a different tutorial algorithm."""
import subprocess
import sys
import unittest
from pathlib import Path

class NotebookParityTests(unittest.TestCase):
    def test_generated_algorithms_match_published_notebooks(self):
        script = Path(__file__).resolve().parents[1] / 'tools/sync_notebook_code.py'
        subprocess.run([sys.executable, str(script), '--check'], check=True)
