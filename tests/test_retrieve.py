import tempfile

import pytest

from patapsco.topics import Query
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
        pr = BM25Retriever(run_path=self.temp_dir, config=conf)
        pr.begin()
        assert pr.lang == "rus"

    def test_no_lang(self):
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir))))
        pr = BM25Retriever(run_path=self.temp_dir, config=conf)
        with pytest.raises(PatapscoError):
            pr.begin()

    def _prepare_cacm_index(self):
        # downloads an index from anserini to test against
        import tarfile
        from urllib.request import urlretrieve
        url = 'https://github.com/castorini/anserini-data/raw/master/CACM/lucene-index.cacm.tar.gz'
        tarball_path = str(self.temp_dir / 'lucene-index.cacm.tar.gz')
        _, _ = urlretrieve(url, tarball_path)
        tarball = tarfile.open(tarball_path)
        tarball.extractall(self.temp_dir)
        tarball.close()
        index_path = self.temp_dir / 'lucene-index.cacm'
        lang_path = index_path / ".lang"
        lang_path.write_text("eng")
        return index_path

    @pytest.mark.slow(reason="downloads pre-built index to validate against pyserini")
    def test_sparse_retrieval(self):
        # check equivalence against pyserini results up to 5 digits
        # see https://github.com/castorini/pyserini/blob/3cd6b7ee8e77d699726756938fac0714c10ad0a9/tests/test_index_reader.py#L33
        index_path = self._prepare_cacm_index()
        query = Query("123", "eng", query="inform retriev", text="", report=None)
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(index_path))))

        conf.name = "bm25"
        bm25 = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        results = bm25.process(query)
        assert 'CACM-3134' == results.results[0].doc_id
        assert pytest.approx(4.76550, results.results[0].score, 1e-5)

        conf.name = "bm25"
        conf.rm3 = True
        rm3 = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        results = rm3.process(query)
        assert 'CACM-3134' == results.results[0].doc_id
        assert pytest.approx(2.18010, results.results[0].score, 1e-5)

        conf.name = "qld"
        conf.rm3 = False
        qld = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        results = qld.process(query)
        assert 'CACM-3134' == results.results[0].doc_id
        assert pytest.approx(3.68030, results.results[0].score, 1e-5)
