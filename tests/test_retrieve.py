import tempfile

import pytest

from patapsco.retrieve import *
from patapsco.schema import PathConfig, RetrieveInputConfig
from patapsco.util.file import delete_dir


class TestPyseriniRetriever:
    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_lang(self):
        lang_path = self.temp_dir / ".lang"
        lang_path.write_text("rus")
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir))))
        pr = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        pr.begin()
        assert pr.lang == "rus"

    def test_no_lang(self):
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir))))
        pr = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        with pytest.raises(PatapscoError):
            pr.begin()
