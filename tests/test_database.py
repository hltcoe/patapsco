import json
import pathlib
import sqlite3
import tempfile

from patapsco.database import DocumentDatabase
from patapsco.docs import Doc
from patapsco.util.file import delete_dir


class TestDocumentDatabase:
    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_adding_doc(self):
        db = DocumentDatabase(self.temp_dir, 'database', readonly=False)
        db['doc1'] = Doc('doc1', lang='eng', text='hello world', title='foo bar baz', date='2020-12-25')
        doc = db['doc1']
        assert doc.id == 'doc1'
        assert doc.date == '2020-12-25'
        assert doc.lang == 'eng'
        assert doc.text == 'hello world'

        # and check retrieving from file
        conn = sqlite3.connect(str(pathlib.Path(self.temp_dir) / 'database' / 'docs.db'))
        cursor = conn.execute(f'SELECT value FROM patapsco WHERE key = ?', ('doc1',))
        result = cursor.fetchone()
        assert result is not None
        loaded_doc = json.loads(result[0])
        assert loaded_doc['id'] == doc.id
        assert loaded_doc['date'] == doc.date
        assert loaded_doc['lang'] == doc.lang
        assert loaded_doc['text'] == doc.text
