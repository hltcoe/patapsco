

class PatapscoError(Exception):
    pass


class ConfigError(PatapscoError):
    pass


class ParseError(PatapscoError):
    pass


class BadDataError(PatapscoError):
    pass
