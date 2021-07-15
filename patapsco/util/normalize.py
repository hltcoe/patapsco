"""
Library for normalizing text

Types of normalization:

# Corruption
1. Incorrect encodings. For example, latin-1 encoded text in a utf-8 file.
2. Escaped characters. For example, &amp; from html in a utf-8 file.
3. HTML or XML tags in the text from a poor extraction process.
Note 2 and 3 are not implemented as not found to be an issue with our data.
There are cases of #2, but they were escaped multiple times.

# Standardization
1. Use a single representation for punctuation (quotes, dashes, periods, commas, etc.).
2. Use a single representation for symbols (percent sign, etc.).
3. Use a single representation for spacing (newlines, tabs, various spaces).
4. Use a common representation for numbers.
5. Case folding (lowercasing).

# Remove Formatting Characters
1. Directionality markers (LTR mark, RTL mark, Pop directionality, etc.).
2. Joiners (Zero Width Non-Joiner, Word Joiner, etc.).

# Diacritics
1. Condense combining diacritics.
2. Remove diacritics.

# Remove
1. Control characters
2. Punctuation
3. Emojis
4. Characters outside of the expected unicode block(s)

# Replace
1. Numbers
2. Phone numbers
3. URLs
4. Usernames


# Unicode characters for normalization

## Quotes
 - Double quote \u0022
 - Single quote \0027 (apostrophe)
 - Left double angle quote \u00ab
 - Right double angle quote \u00bb
 - Left single quote \u2018
 - Right single quote \u2019
 - Single low quote \u201a
 - Single high quote \u201b
 - Left double quote \u201c
 - Right double quote \u201d
 - Double low quote \u201e
 - Double high quote \u201f
 - Left single angle quote \u2039
 - Right single angle quote \u203a
 - Double prime \u2033
 - Left corner bracket \u300c
 - Right corner bracket \u300d
 - Left white corner bracket \u300e
 - Right white corner bracket \u200b

## Parentheses and brackets
 - Left parenthesis \u0028
 - Right parenthesis \u0029
 - Left square bracket \u005b
 - Right square bracket \u005d
 - Left curly bracket \u007b
 - Right curly bracket \u007d
 - Left double angle bracket \u300a
 - Right double angle bracket \u300b
 - Left white lenticular bracket \u3016
 - Right white lenticular bracket \u3017
 - Ornate left parenthesis \ufd3e
 - Ornate right parenthesis \ufd3f

## Dashes
 - Hyphen minus \u002d
 - Hyphen \u2010
 - Non-breaking hyphen \u2011
 - Figure dash \u2012
 - En dash \u2013
 - Em dash \u2014
 - Horizontal dash \u2015
 - Fullwidth hyphen minus \uff0d

## Periods
 - Full stop \u002e
 - Arabic full stop \u06d4

## Commas
 - Comma \u002c
 - Arabic comma \u060c

## Colons
 - Colon \u003a
 - Semicolon \u003b
 - Arabic semicolon \u061b

## Spaces
 - Line feed \u000a
 - Carriage return \u000d
 - Space \u0020
 - Thin space \u2009
 - Hair space \u200a
 - Other spaces \u2000-\u2008
 - Line separator \u2028
 - Paragraph separator \u2029
 - Ideographic space \u3000

## Directionality
 - LTR mark \u200e
 - RTL mark \u200f
 - LTR embedding \u202a
 - RTL embedding \u202b
 - Pop direction \u202c
 - LTR override \u202d
 - RTL override \u202e
 - LTR isolate \u2066
 - RTL isolate \u2067
 - Pop direction isolate \u2069
 - Inhibit symmetric swapping \u206a (change mapping of left/right to open/close for parentheses or similar)
 - Activate symmetric swapping \u206b

## Other formatting
 - Arabic letter mark \u061c
 - Inhibit Arabic form shaping \u206c
 - Activate Arabic form shaping \u206d
 - National digit shapes \u206e
 - Nominal digit shapes \u206f
 - Variation selector \ufe01 - \ufe0f (used to select glyph for preceding character)

## Joiners and related
 - No break space \u00a0
 - Zero width space \u200b
 - Zero width non-joiner \u200c
 - Zero width joiner \u200d
 - Word joiner \u2060
 - Function application \u2061
 - Invisible separator \u2063
 - Soft hyphen \u00ad (indicates possible line break location)

## Numbers
 - Latin \u0030 - \u0039
 - Arabic \u0660 - \u0669
 - Extended Arabic \u06f0 - \u06f9
 - Roman \u2160 - \u2169
 - Hangzhou \u3020 - \u3029
 - Tibetan \uff20 - \uff29
 - Fullwidth \uff10 - \uff19
 - Ideographic number zero \u3007
"""

import collections
import difflib
import re
import sys
import unicodedata

import ftfy


def compare_strings(s1, s2):
    """Compare two strings to determine what has changed

    Returns:
        dict of change -> count
    """
    changes = collections.Counter()
    matcher = difflib.SequenceMatcher(None, s1, s2)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        # only consider single letter changes
        if tag == 'delete' and i2 - i1 == 1:
            changes.update({f"del {s1[i1:i2]}": 1})
        elif tag == 'replace' and i2 - i1 <= 2 and j2 - j1 == 1:
            changes.update({f"{s1[i1:i2]} → {s2[j1:j2]}": 1})
        elif tag == 'insert' and j2 - j1 == 1:
            changes.update({f"ins {s2[j1:j2]}": 1})
    return changes


class NormalizerFactory:
    classes = {
        'ara': 'ArabicNormalizer',
        'eng': 'EnglishNormalizer',
        'fas': 'FarsiNormalizer',
        'rus': 'RussianNormalizer',
        'zho': 'ChineseNormalizer',
    }

    @classmethod
    def create(cls, lang, config):
        if lang in cls.classes:
            namespace = vars(sys.modules[cls.__module__])
            return namespace[cls.classes[lang]](config)
        else:
            raise ValueError(f"Unknown language: {lang}")


class Normalizer:
    """Base class of the text normalizers"""

    FORMAT_RANGE = [
        '\u200e', '\u200f', '\u202a-\u202e', '\u2066-\u206b',  # RTL
        '\u061c', '\u206c-\u206f',  # Arabic shaping and national digit selection
        '\ufe01-\ufe0f',  # Variation selectors
        '\u00a0', '\u00ad', '\u200b-\u200d', '\u2060-\u2063',  # Joiners, non-joiners, etc.
    ]

    def __init__(self, config):
        self.config = config
        format_chars = ''.join(self._expand_chars(x) for x in self.FORMAT_RANGE)
        self.format_trans = str.maketrans('', '', format_chars)

    @staticmethod
    def _expand_chars(chars_range):
        if '-' in chars_range:
            start, stop = chars_range.split('-')
            return ''.join(chr(item) for item in range(ord(start), ord(stop) + 1))
        else:
            return chars_range

    @staticmethod
    def update_spaces(text):
        return re.sub(r'\s+', ' ', text)

    def remove_format_chars(self, text):
        return text.translate(self.format_trans)

    @staticmethod
    def remove_control_chars(text):
        return ''.join(char for char in text if char.isprintable())

    @staticmethod
    def fix_encoding(text):
        return ftfy.fix_encoding(text)

    @staticmethod
    def standardize_combining_chars(text):
        """combine chars + separate diacritics"""
        return unicodedata.normalize('NFC', text)

    @staticmethod
    def standardize_quotes(text):
        return ftfy.fixes.uncurl_quotes(text)


class GenericNormalizer(Normalizer):
    """General text normalizer"""

    def pre_normalize(self, text):
        """Normalization common to all processing"""
        text = self.fix_encoding(text)
        text = self.update_spaces(text)
        text = self.remove_control_chars(text)
        text = self.remove_format_chars(text)
        return self.standardize_combining_chars(text)

    def post_normalize(self, text):
        """Normalization for indexing

        This could operate on token text or entire an document.
        """
        if self.config.lowercase:
            return text.lower()
        return text


class ArabicNormalizer(GenericNormalizer):
    pass


class ChineseNormalizer(GenericNormalizer):
    pass


class EnglishNormalizer(GenericNormalizer):
    pass


class FarsiNormalizer(GenericNormalizer):
    pass


class RussianNormalizer(GenericNormalizer):
    pass
