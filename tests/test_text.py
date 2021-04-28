import pytest

from patapsco.text import *


def test_stop_words_english():
    swr = StopWordsRemoval('lucene', 'en')
    text = swr.remove(['this', 'is', 'a', 'test'])
    assert text == ['test']


def test_stop_words_english_case():
    swr = StopWordsRemoval('lucene', 'en')
    text = swr.remove(['This', 'is', 'a', 'test'])
    assert text == ['This', 'test']
    text = swr.remove(['This', 'is', 'a', 'test'], lower=True)
    assert text == ['test']


@pytest.mark.skip(reason="Slow test")
def test_tokenizer_english():
    tokenizer = StanzaTokenizer(config=None, lang='en')
    tokens = tokenizer.tokenize("Mary had a little lamb.")
    assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']
