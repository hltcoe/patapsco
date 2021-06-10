import tempfile

from patapsco.docs import Doc, DocumentDatabase
from patapsco.util.file import delete_dir


class TestDocumentDatabase:
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_adding_doc(self):
        db = DocumentDatabase(self.temp_dir, 'database', None, readonly=False)
        db['doc1'] = Doc('doc1', lang='eng', text='hello world', date='2020-12-25')
        doc = db['doc1']
        assert doc.id == 'doc1'
        assert doc.date == '2020-12-25'
        assert doc.lang == 'eng'
        assert doc.text == 'hello world'
