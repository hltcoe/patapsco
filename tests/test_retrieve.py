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

    @pytest.mark.slow(reason="downloads pre-built index to validate against pyserini")
    def test_sparse_retrieval(self):
        # see https://github.com/castorini/pyserini/blob/3cd6b7ee8e77d699726756938fac0714c10ad0a9/tests/test_index_reader.py#L33
        import tarfile
        from math import isclose
        from pyserini import index, search
        from random import randint
        from urllib.request import urlretrieve
        r = randint(0, 10000000)
        url = 'https://github.com/castorini/anserini-data/raw/master/CACM/lucene-index.cacm.tar.gz'
        tarball_name = 'lucene-index.cacm-{}.tar.gz'.format(r)
        index_dir = self.temp_dir / 'index{}'.format(r)
        _, _ = urlretrieve(url, tarball_name)
        tarball = tarfile.open(tarball_name)
        tarball.extractall(index_dir)
        tarball.close()
        index_path = index_dir / 'lucene-index.cacm'
        lang_path = self.temp_dir / ".lang"
        lang_path.write_text("eng")
        conf = RetrieveConfig(name="rm3", input=RetrieveInputConfig(index=PathConfig(path=str(index_path))))
        bm25 = BM25Retriever(run_path=self.temp_dir, config=conf)
        qld = QLDRetriever(run_path=self.temp_dir, config=conf)
        rm3 = RM3Retriever(run_path=self.temp_dir, config=conf)
        pr = BM25Retriever(run_path=self.temp_dir, config=conf)
        query = Query(123, "eng", "inform retriev", "", report=None)
        # check equivalence against pyserini results up to 5 digits
        assert isclose(bm25.process(query).results[0].score, 4.76550, abs_tol=10**-5)
        assert isclose(qld.process(query).results[0].score, 3.68030, abs_tol=10**-5)
        assert isclose(rm3.process(query).results[0].score, 2.18010, abs_tol=10**-5)
