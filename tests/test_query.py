import pipeline.query as query


def test_select_text():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = query.Topic('1', 'en', 'title', 'desc', 'narr')
    text = query.QueryProcessor._select_text(mock, topic)
    assert text == "title desc"
