import pytest

from patapsco.schema import TextProcessorConfig
from patapsco.text import *


def test_stop_words_english():
    swr = StopWordsRemover('lucene', 'eng')
    tokens = ['this', 'is', 'a', 'test']
    indices = swr.identify(tokens)
    new_tokens = swr.remove(tokens, indices)
    assert new_tokens == ['test']


def test_stop_words_english_uppercase():
    swr = StopWordsRemover('lucene', 'eng')
    tokens = ['This', 'is', 'a', 'test']
    indices = swr.identify(tokens, is_lower=True)
    new_tokens = swr.remove(tokens, indices)
    assert new_tokens == ['This', 'test']


def test_stop_words_english_caseless():
    swr = StopWordsRemover('lucene', 'eng')
    tokens = ['This', 'is', 'a', 'test']
    indices = swr.identify(tokens, is_lower=False)
    new_tokens = swr.remove(tokens, indices)
    assert new_tokens == ['test']


def test_porter_stemmer_english():
    tokens = ['It', 'was', 'a', 'bright', 'cold', 'day', 'in', 'April', ',', 'and', 'the', 'clocks', 'were', 'striking', 'thirteen', '.']
    ans = ['It', 'wa', 'a', 'bright', 'cold', 'day', 'in', 'April', ',', 'and', 'the', 'clock', 'were', 'strike', 'thirteen', '.']
    stemmer = PorterStemmer("eng")
    assert ans == stemmer.stem(tokens)


def test_porter_stemmer_long_token():
    tokens = ['It', 'HYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY', 'hello']
    tokens = [tok.lower() for tok in tokens]
    stemmer = PorterStemmer("eng")
    assert tokens == stemmer.stem(tokens)


class TestTokenizerStemmerFactory:
    def setup_method(self):
        TokenizerStemmerFactory.tokenizer_cache = {}
        TokenizerStemmerFactory.stemmer_cache = {}

    def test_validate_with_invalid_tokenize(self):
        with pytest.raises(ConfigError, match="Unknown tokenizer"):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="stanford", stem=False), "eng")

    def test_validate_with_invalid_stem(self):
        with pytest.raises(ConfigError, match="Unknown stemmer"):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="whitespace", stem="invalid"), "eng")

    def test_validate_ngram_with_stem(self):
        with pytest.raises(ConfigError):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="ngram", stem="porter"), "eng")

    def test_validate_whitespace_with_invalid_stem(self):
        with pytest.raises(ConfigError):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="whitespace", stem="stanza"), "eng")

    def test_validate_whitespace_with_valid_stem(self):
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="whitespace", stem="porter"), "eng")

    def test_validate_stanza_with_spacy(self):
        with pytest.raises(ConfigError):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="stanza", stem="spacy"), "eng")

    def test_validate_stanza_porter_en(self):
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="stanza", stem="porter"), "eng")

    def test_validate_parsivar(self):
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="moses", stem="parsivar"), "fas")
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="spacy", stem="parsivar"), "fas")
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="stanza", stem="parsivar"), "fas")
        TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="whitespace", stem="parsivar"), "fas")
        with pytest.raises(ConfigError):
            TokenizerStemmerFactory.validate(TextProcessorConfig(tokenize="ngram", stem="parsivar"), "fas")

    def test_create_tokenizer_whitespace(self):
        conf = TextProcessorConfig(tokenize="whitespace", stem=False)
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert isinstance(tokenizer, WhiteSpaceTokenizer)

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_ngram(self):
        conf = TextProcessorConfig(tokenize="ngram", stem=False)
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert isinstance(tokenizer, NgramTokenizer)

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_moses(self):
        conf = TextProcessorConfig(tokenize="moses", stem=False)
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert isinstance(tokenizer, MosesTokenizer)

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_caching(self):
        conf = TextProcessorConfig(tokenize="moses", stem=False)
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        tokenizer2 = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert tokenizer == tokenizer2

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_spacy_no_stem(self):
        conf = TextProcessorConfig(tokenize="spacy", stem=False)
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert isinstance(tokenizer, SpacyNLP)
        assert 'tok2vec' in tokenizer.nlp.disabled

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_spacy_with_stem(self):
        conf = TextProcessorConfig(tokenize="spacy", stem="spacy")
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        assert isinstance(tokenizer, SpacyNLP)
        assert 'tok2vec' not in tokenizer.nlp.disabled

    def test_create_stemmer_porter(self):
        conf = TextProcessorConfig(tokenize="spacy", stem="porter")
        stemmer = TokenizerStemmerFactory.create_stemmer(conf, "eng")
        assert isinstance(stemmer, PorterStemmer)

    @pytest.mark.slow(reason="loads spacy model")
    def test_create_tokenizer_stemmer_combo(self):
        conf = TextProcessorConfig(tokenize="spacy", stem="spacy")
        tokenizer = TokenizerStemmerFactory.create_tokenizer(conf, "eng")
        stemmer = TokenizerStemmerFactory.create_stemmer(conf, "eng")
        assert tokenizer == stemmer


class TestStanza:
    model_path = pathlib.Path.home() / 'stanza_resources'

    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = StanzaNLP(lang='ara', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.skip(reason="stanza arabic model struggles - may be problem with foreign words")
    def test_stemmer_arabic(self):
        text = 'فيلم جاذبية يتصدر ترشيحات جوائز الأكاديمية البريطانية لفنون الفيلم والتلفزيون.'
        ans = ['فيلم', 'جاذبية', 'تصدر', 'ترشيح', 'جائزة', 'أكاديمية', 'بريطاني', 'فن', 'فيلم', 'تلفزيون', '.']
        nlp = StanzaNLP(lang='ara', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = StanzaNLP(lang='zho', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        # jieba is splitting 要看
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = StanzaNLP(lang='eng', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_stemmer_english(self):
        text = 'It\'s fleece was white as snow.'
        ans = ['it', "'s", 'fleece', 'be', 'white', 'as', 'snow', '.']
        nlp = StanzaNLP(lang='eng', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = StanzaNLP(lang='fas', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_stemmer_farsi(self):
        text = 'چگونه می‌توان با جاماسپ در نقشه‌ها پیمایش کرد؟'
        ans = ['چگونه', '#توان', 'با', 'جاماسپ', 'در', 'نقشه', 'پیمایش', 'کرد#کن', '؟']
        nlp = StanzaNLP(lang='fas', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = StanzaNLP(lang='rus', model_path=self.model_path, stem=False)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        # Does the Russian model not handle punctuation well or did we hit on a bad sentence
        assert tokens == ['Я', 'хотел', 'бы', 'пива' , '.']

    @pytest.mark.slow
    def test_stemmer_russian(self):
        text = 'Новые расходы финансируются благодаря крупным суммам на банковском счету Клинтон.'
        ans = ['новый', 'расход', 'финансировать', 'благодаря', 'крупный', 'сумма', 'на', 'банковский', 'счет', 'Клинтон', '.']
        nlp = StanzaNLP(lang='rus', model_path=self.model_path, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)


class TestSpacy:
    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        tokenizer = SpacyNLP(lang='ara', model_path=None, stem=False)
        tokens = tokenizer.tokenize("في أسرتي ثلاثة أفراد.")
        assert tokens == ['في', 'أسرتي', 'ثلاثة', 'أفراد', '.']

    @pytest.mark.slow
    def test_tokenizer_chinese(self):
        tokenizer = SpacyNLP(lang='zho', model_path=None, stem=False)
        tokens = tokenizer.tokenize("不但要看,而且要帮。")
        assert tokens == ['不但', '要', '看', ',', '而且', '要', '帮',  '。']

    @pytest.mark.slow
    def test_tokenizer_english(self):
        tokenizer = SpacyNLP(lang='eng', model_path=None, stem=False)
        tokens = tokenizer.tokenize("Mary had a little lamb.")
        assert tokens == ['Mary', 'had', 'a', 'little', 'lamb', '.']

    @pytest.mark.slow
    def test_stemmer_english(self):
        text = 'A witness told police that the victim had attacked the suspect in April.'
        ans = ['a', "witness", 'tell', 'police', 'that', 'the', 'victim', 'have', 'attack', 'the', 'suspect', 'in', 'April', '.']
        nlp = SpacyNLP(lang='eng', model_path=None, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        tokenizer = SpacyNLP(lang='fas', model_path=None, stem=False)
        tokens = tokenizer.tokenize("شما بليز رو به فارسی چی میگین؟")
        assert tokens == ['شما', 'بليز', 'رو', 'به', 'فارسی', 'چی', 'میگین', '؟']

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        tokenizer = SpacyNLP(lang='rus', model_path=None, stem=False)
        tokens = tokenizer.tokenize("Я хотел бы пива.")
        assert tokens == ['Я', 'хотел', 'бы', 'пива', '.']

    @pytest.mark.slow
    def test_stemmer_russian(self):
        text = 'Новые расходы финансируются благодаря крупным суммам на банковском счету Клинтон.'
        ans = ['новый', 'расход', 'финансироваться', 'благодаря', 'крупный', 'сумма', 'на', 'банковский', 'счёт', 'клинтон', '.']
        nlp = SpacyNLP(lang='rus', model_path=None, stem=True)
        tokens = nlp.tokenize(text)
        assert ans == nlp.stem(tokens)


class TestMoses:
    @pytest.mark.slow
    def test_tokenizer_arabic(self):
        text = "تمول النفقات الجديدة من خلال حساب كلينتون المصرفي الكبير. الحد الأقصى المسموح به للشخص الواحد هو 5000 دولار."
        ans = [
            'تمول', 'النفقات', 'الجديدة', 'من', 'خلال', 'حساب', 'كلينتون', 'المصرفي', 'الكبير', '.',
            'الحد', 'الأقصى', 'المسموح', 'به', 'للشخص', 'الواحد', 'هو', '5000', 'دولار', '.'
        ]
        tokenizer = MosesTokenizer(lang='ara', model_path=None)
        assert tokenizer.tokenize(text) == ans

    def test_tokenizer_chinese(self):
        with pytest.raises(ConfigError):
            MosesTokenizer(lang='zho', model_path=None)

    @pytest.mark.slow
    def test_tokenizer_english(self):
        text = "Mary had a little lamb. It's fleece was white as snow."
        ans = [
            'Mary', 'had', 'a', 'little', 'lamb', '.',
            'It', "'s", 'fleece', 'was', 'white', 'as', 'snow', '.'
        ]
        tokenizer = MosesTokenizer(lang='eng', model_path=None)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_farsi(self):
        text = "بلیت را پیشفروش کنید. این موافقتنامه را امضا نخواهم کرد و تا جایی که بتوانم در مقابل آن پایداری میکنم."
        ans = [
            'بلیت', 'را', 'پیشفروش', 'کنید', '.',
            'این', 'موافقتنامه', 'را', 'امضا', 'نخواهم', 'کرد', 'و', 'تا', 'جایی', 'که', 'بتوانم', 'در', 'مقابل', 'آن', 'پایداری', 'میکنم', '.',
        ]
        tokenizer = MosesTokenizer(lang='fas', model_path=None)
        assert ans == tokenizer.tokenize(text)

    @pytest.mark.slow
    def test_tokenizer_russian(self):
        text = "Свидетель рассказал в полиции, что потерпевший напал на подозреваемого в апреле. Нужно провести параллель между играми и нашей повседневной жизнью."
        ans = [
            'Свидетель', 'рассказал', 'в', 'полиции', ',', 'что', 'потерпевший', 'напал', 'на', 'подозреваемого', 'в', 'апреле', '.',
            'Нужно', 'провести', 'параллель', 'между', 'играми', 'и', 'нашей', 'повседневной', 'жизнью', '.',
        ]
        tokenizer = MosesTokenizer(lang='rus', model_path=None)
        assert ans == tokenizer.tokenize(text)


class TestNgramTokenizer:
    @pytest.mark.slow
    def test_stanza_tokenizer_english(self):
        text = "Roses are red. Violets are blue."
        ans = [
            'Roses', 'oses ', 'ses a', 'es ar', 's are', ' are ', 'are r', 're re', 'e red', ' red.',
            'Viole', 'iolet', 'olets', 'lets ', 'ets a', 'ts ar', 's are', ' are ', 'are b', 're bl', 'e blu', ' blue', 'blue.'
        ]
        tokenizer = NgramTokenizer(lang='eng', model_path=None)
        assert ans == tokenizer.tokenize(text)
