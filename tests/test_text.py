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


class TestStanza:
    model_path = str(pathlib.Path.home() / 'stanza_resources')

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = StanzaTokenizer(config=None, lang='ar', model_path=self.model_path)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = StanzaTokenizer(config=None, lang='zh', model_path=self.model_path)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        # jieba is splitting 要看
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = StanzaTokenizer(config=None, lang='en', model_path=self.model_path)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = StanzaTokenizer(config=None, lang='fa', model_path=self.model_path)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = StanzaTokenizer(config=None, lang='ru', model_path=self.model_path)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        # Does the Russian model not handle punctuation well or did we hit on a bad sentence
        assert tokens == ['Я', 'хотел', 'бы', 'пива.']


class TestSpacy:
    # if not running on the grid, spacy will use pip to install models
    model_path = '/exp/scale21/resources/spacy'

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = SpaCyTokenizer(config=None, lang='ar', model_path=self.model_path)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = SpaCyTokenizer(config=None, lang='zh', model_path=self.model_path)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = SpaCyTokenizer(config=None, lang='en', model_path=self.model_path)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = SpaCyTokenizer(config=None, lang='fa', model_path=self.model_path)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = SpaCyTokenizer(config=None, lang='ru', model_path=self.model_path)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        assert tokens == ['Я', 'хотел', 'бы', 'пива', '.']


class TestMoses:
    # moses requires the sentence segmentation from spaCy
    model_path = '/exp/scale21/resources/spacy'

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        text = "تمول النفقات الجديدة من خلال حساب كلينتون المصرفي الكبير. الحد الأقصى المسموح به للشخص الواحد هو 5000 دولار."
        ans = [
            'تمول', 'النفقات', 'الجديدة', 'من', 'خلال', 'حساب', 'كلينتون', 'المصرفي', 'الكبير', '.',
            'الحد', 'الأقصى', 'المسموح', 'به', 'للشخص', 'الواحد', 'هو', '5000', 'دولار', '.'
        ]
        tokenizer = MosesTokenizer(config=None, lang='ar', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    def test_tokenizer_chinese(self):
        with pytest.raises(ConfigError):
            MosesTokenizer(config=None, lang='zh', model_path=self.model_path)

    @pytest.mark.slow
    def test_tokenizer_english(self):
        text = "Mary had a little lamb. It's fleece was white as snow."
        ans = [
            'Mary', 'had', 'a', 'little', 'lamb', '.',
            'It', "'s", 'fleece', 'was', 'white', 'as', 'snow', '.'
        ]
        tokenizer = MosesTokenizer(config=None, lang='en', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        text = "بلیت را پیشفروش کنید. این موافقتنامه را امضا نخواهم کرد و تا جایی که بتوانم در مقابل آن پایداری میکنم."
        ans = [
            'بلیت', 'را', 'پیشفروش', 'کنید', '.',
            'این', 'موافقتنامه', 'را', 'امضا', 'نخواهم', 'کرد', 'و', 'تا', 'جایی', 'که', 'بتوانم', 'در', 'مقابل', 'آن', 'پایداری', 'میکنم', '.',
        ]
        tokenizer = MosesTokenizer(config=None, lang='fa', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        text = "Свидетель рассказал в полиции, что потерпевший напал на подозреваемого в апреле. Нужно провести параллель между играми и нашей повседневной жизнью."
        ans = [
            'Свидетель', 'рассказал', 'в', 'полиции', ',', 'что', 'потерпевший', 'напал', 'на', 'подозреваемого', 'в', 'апреле', '.',
            'Нужно', 'провести', 'параллель', 'между', 'играми', 'и', 'нашей', 'повседневной', 'жизнью', '.',
        ]
        tokenizer = MosesTokenizer(config=None, lang='ru', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)


class TestNgramTokenizer:
    # ngram tokenization uses sentence segmentation from spaCy
    model_path = '/exp/scale21/resources/spacy'

    @pytest.mark.slow
    def test_stanza_tokenizer_english(self):
        text = "Roses are red. Violets are blue."
        ans = [
            'Roses', 'oses ', 'ses a', 'es ar', 's are', ' are ', 'are r', 're re', 'e red', ' red.',
            'Viole', 'iolet', 'olets', 'lets ', 'ets a', 'ts ar', 's are', ' are ', 'are b', 're bl', 'e blu', ' blue', 'blue.'
        ]
        tokenizer = NgramTokenizer(config=None, lang='en', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)
