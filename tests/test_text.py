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


def test_porter_stemmer_english():
    tokens = ['It', 'was', 'a', 'bright', 'cold', 'day', 'in', 'April', ',', 'and', 'the', 'clocks', 'were', 'striking', 'thirteen', '.']
    ans = ['It', 'wa', 'a', 'bright', 'cold', 'day', 'in', 'April', ',', 'and', 'the', 'clock', 'were', 'strike', 'thirteen', '.']
    stemmer = PorterStemmer("en")
    assert ans == stemmer.stem(tokens)


class TestStanza:
    model_path = str(pathlib.Path.home() / 'stanza_resources')

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = StanzaNLP(lang='ar', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.skip(reason="stanza arabic model struggles - may be problem with foreign words")
    def test_stemmer_arabic(self):
        text = 'فيلم جاذبية يتصدر ترشيحات جوائز الأكاديمية البريطانية لفنون الفيلم والتلفزيون.'
        ans = ['فيلم', 'جاذبية', 'تصدر', 'ترشيح', 'جائزة', 'أكاديمية', 'بريطاني', 'فن', 'فيلم', 'تلفزيون', '.']
        nlp = StanzaNLP(lang='ar', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = StanzaNLP(lang='zh', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        # jieba is splitting 要看
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = StanzaNLP(lang='en', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_stemmer_english(self):
        text = 'It\'s fleece was white as snow.'
        ans = ['it', "'s", 'fleece', 'be', 'white', 'as', 'snow', '.']
        nlp = StanzaNLP(lang='en', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = StanzaNLP(lang='fa', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_stemmer_farsi(self):
        text = 'چگونه می‌توان با جاماسپ در نقشه‌ها پیمایش کرد؟'
        ans = ['چگونه', '#توان', 'با', 'جاماسپ', 'در', 'نقشه', 'پیمایش', 'کرد#کن', '؟']
        nlp = StanzaNLP(lang='fa', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = StanzaNLP(lang='ru', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        # Does the Russian model not handle punctuation well or did we hit on a bad sentence
        assert tokens == ['Я', 'хотел', 'бы', 'пива.']

    @pytest.mark.slow
    def test_stemmer_russian(self):
        text = 'Новые расходы финансируются благодаря крупным суммам на банковском счету Клинтон.'
        ans = ['новый', 'расход', 'финансировать', 'благодаря', 'крупный', 'сумма', 'на', 'банковский', 'счет', 'Клинтон', '.']
        nlp = StanzaNLP(lang='ru', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)


class TestSpacy:
    # if not running on the grid, spacy will use pip to install models
    model_path = '/exp/scale21/resources/spacy'

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = SpacyNLP(lang='ar', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = SpacyNLP(lang='zh', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = SpacyNLP(lang='en', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_stemmer_english(self):
        text = 'A witness told police that the victim had attacked the suspect in April.'
        ans = ['a', "witness", 'tell', 'police', 'that', 'the', 'victim', 'have', 'attack', 'the', 'suspect', 'in', 'April', '.']
        nlp = SpacyNLP(lang='en', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = SpacyNLP(lang='fa', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = SpacyNLP(lang='ru', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        assert tokens == ['Я', 'хотел', 'бы', 'пива', '.']

    @pytest.mark.slow
    def test_stemmer_russian(self):
        text = 'Новые расходы финансируются благодаря крупным суммам на банковском счету Клинтон.'
        ans = ['новый', 'расход', 'финансироваться', 'благодаря', 'крупный', 'сумма', 'на', 'банковский', 'счёт', 'клинтон', '.']
        nlp = SpacyNLP(lang='ru', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)


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
        tokenizer = MosesTokenizer(lang='ar', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    def test_tokenizer_chinese(self):
        with pytest.raises(ConfigError):
            MosesTokenizer(lang='zh', model_path=self.model_path)

    @pytest.mark.slow
    def test_tokenizer_english(self):
        text = "Mary had a little lamb. It's fleece was white as snow."
        ans = [
            'Mary', 'had', 'a', 'little', 'lamb', '.',
            'It', "'s", 'fleece', 'was', 'white', 'as', 'snow', '.'
        ]
        tokenizer = MosesTokenizer(lang='en', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        text = "بلیت را پیشفروش کنید. این موافقتنامه را امضا نخواهم کرد و تا جایی که بتوانم در مقابل آن پایداری میکنم."
        ans = [
            'بلیت', 'را', 'پیشفروش', 'کنید', '.',
            'این', 'موافقتنامه', 'را', 'امضا', 'نخواهم', 'کرد', 'و', 'تا', 'جایی', 'که', 'بتوانم', 'در', 'مقابل', 'آن', 'پایداری', 'میکنم', '.',
        ]
        tokenizer = MosesTokenizer(lang='fa', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        text = "Свидетель рассказал в полиции, что потерпевший напал на подозреваемого в апреле. Нужно провести параллель между играми и нашей повседневной жизнью."
        ans = [
            'Свидетель', 'рассказал', 'в', 'полиции', ',', 'что', 'потерпевший', 'напал', 'на', 'подозреваемого', 'в', 'апреле', '.',
            'Нужно', 'провести', 'параллель', 'между', 'играми', 'и', 'нашей', 'повседневной', 'жизнью', '.',
        ]
        tokenizer = MosesTokenizer(lang='ru', model_path=self.model_path)
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
        tokenizer = NgramTokenizer(lang='en', model_path=self.model_path)
        assert ans == tokenizer.tokenize(text)
