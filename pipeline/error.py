

class PipelineError(Exception):
    pass


class ConfigError(PipelineError):
    pass


class ParseError(PipelineError):
    pass
