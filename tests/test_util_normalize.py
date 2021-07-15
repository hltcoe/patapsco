from patapsco.schema import NormalizationConfig
from patapsco.util.normalize import *


class TestCompare:
    def test_same(self):
        results = compare_strings("hello world!", "hello world!")
        assert len(results) == 0

    def test_simple_delete(self):
        results = compare_strings("hello world!", "hello world")
        assert len(results) == 1
        assert results['del !'] == 1

    def test_simple_replace(self):
        results = compare_strings("\u0043\u0327", "Ç")
        assert len(results) == 1
        assert results['\u0043\u0327 → \u00c7'] == 1


class TestNormalizer:
    def test_pre_normalize_newlines(self):
        normalizer = NormalizerFactory.create("eng", NormalizationConfig(lowercase=False))
        assert normalizer.pre_normalize("line1\nline2") == "line1 line2"

    def test_post_normalize_lowercase(self):
        normalizer = GenericNormalizer(NormalizationConfig(lowercase=True))
        assert normalizer.post_normalize("Test test") == "test test"

    def test_post_normalize_no_casing(self):
        normalizer = GenericNormalizer(NormalizationConfig(lowercase=False))
        assert normalizer.post_normalize("Test test") == "Test test"

    def test_spaces_tabs(self):
        assert Normalizer.update_spaces("a\t\tb") == "a b"

    def test_multiple_spaces(self):
        assert Normalizer.update_spaces("a    b") == "a b"

    def test_other_spaces(self):
        spaces = [
            '\u000a', '\u000d', '\u0020', '\u2009', '\u200a', '\u2028', '\u2029', '\u3000',
        ]
        for space in spaces:
            assert Normalizer.update_spaces(f"a{space}b") == "a b"

    def test_remove_rtl(self):
        assert Normalizer(NormalizationConfig()).remove_format_chars('a\u200eb') == "ab"
        assert Normalizer(NormalizationConfig()).remove_format_chars('a\u202cb') == "ab"

    def test_remove_control_chars(self):
        assert Normalizer.remove_control_chars("a\uFEFFb") == "ab"

    def test_fix_encoding(self):
        text = "But we\u00e2\u0080\u0099ve come out the other side of it"
        assert Normalizer.fix_encoding(text) == "But we’ve come out the other side of it"

    def test_standardize_combining_chars(self):
        assert Normalizer.standardize_combining_chars("\u0043\u0327") == "\u00c7"  # combine diacritics
        assert Normalizer.standardize_combining_chars("\u2160") != "I"  # do not convert to canonical form (Roman numeral I to capital I)
        assert Normalizer.standardize_combining_chars("\uff0c") == "\uff0c"  # do not convert fullwidth chars to normal chars

    def test_expand_range(self):
        r1 = '\u2000-\u2009'
        r2 = Normalizer._expand_chars(r1)
        assert len(r2) == 10
        assert r2[0] == '\u2000'
        assert r2[1] == '\u2001'
        assert r2[-1] == '\u2009'
