import pipeline.topics as topics


def test_select_text():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = topics.Topic('1', 'en', 'title', 'desc', 'narr')
    text = topics.TopicProcessor._select_text(mock, topic)
    assert text == "title desc"
