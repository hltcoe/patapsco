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


@pytest.mark.slow
def test_stanza_tokenizer_arabic():
    tokenizer = StanzaTokenizer(config=None, lang='ar', model_path='~/stanza_resources')
    tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
    assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']


@pytest.mark.slow
def test_stanza_tokenizer_chinese():
    tokenizer = StanzaTokenizer(config=None, lang='zh', model_path='~/stanza_resources')
    tokens = tokenizer.tokenize("不但要看,而且要帮。")
    # jieba is splitting 要看
    assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']


@pytest.mark.slow
def test_stanza_tokenizer_english():
    tokenizer = StanzaTokenizer(config=None, lang='en', model_path='~/stanza_resources')
    tokens = tokenizer.tokenize("Mary had a little lamb.")
    assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']


@pytest.mark.slow
def test_stanza_tokenizer_farsi():
    tokenizer = StanzaTokenizer(config=None, lang='fa', model_path='~/stanza_resources')
    tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
    assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']


@pytest.mark.slow
def test_stanza_tokenizer_russian():
    tokenizer = StanzaTokenizer(config=None, lang='ru', model_path='~/stanza_resources')
    tokens = tokenizer.tokenize("Я хотел бы пива.")
    # Does the Russian model not handle punctuation well or did we hit on a bad sentence
    assert tokens == ['Я', 'хотел', 'бы', 'пива.']


@pytest.mark.slow
def test_spacy_tokenizer_arabic():
    tokenizer = SpaCyTokenizer(config=None, lang='ar', model_path='/tmp')
    tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
    assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']


@pytest.mark.slow
def test_spacy_tokenizer_chinese():
    tokenizer = SpaCyTokenizer(config=None, lang='zh', model_path='/tmp')
    tokens = tokenizer.tokenize("不但要看,而且要帮。")
    assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']


@pytest.mark.slow
def test_spacy_tokenizer_english():
    tokenizer = SpaCyTokenizer(config=None, lang='en', model_path='/tmp')
    tokens = tokenizer.tokenize("Mary had a little lamb.")
    assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']


@pytest.mark.slow
def test_spacy_tokenizer_farsi():
    tokenizer = SpaCyTokenizer(config=None, lang='fa', model_path='/tmp')
    tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
    assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']


@pytest.mark.slow
def test_spacy_tokenizer_russian():
    tokenizer = SpaCyTokenizer(config=None, lang='ru', model_path='/tmp')
    tokens = tokenizer.tokenize("Я хотел бы пива.")
    assert tokens == ['Я', 'хотел', 'бы', 'пива', '.']
