import enum

from .config import BaseConfig, BaseUncheckedConfig, PathConfig, Optional, Union


class PipelineMode(str, enum.Enum):
    STREAMING = 'streaming'
    BATCH = 'batch'


class Tasks(str, enum.Enum):
    """Tasks that make up the system pipelines"""
    DOCUMENTS = 'documents'
    INDEX = 'index'
    TOPICS = 'topics'
    QUERIES = 'queries'
    RETRIEVE = 'retrieve'
    RERANK = 'rerank'
    SCORE = 'score'


"""""""""""""""""
Text Processing
"""""""""""""""""


class StemConfig(BaseConfig):
    name: str


class TokenizeConfig(BaseConfig):
    name: str


class TextProcessorConfig(BaseConfig):
    """Configuration for the text processing"""
    normalize: bool = True
    tokenize: TokenizeConfig
    lowercase: bool = True
    stopwords: Union[None, bool, str] = "lucene"
    stem: Union[bool, StemConfig] = False
    splits: Optional[list]


"""""""""""""""""
Documents
"""""""""""""""""


class DocumentsInputConfig(BaseConfig):
    """Configuration for the document corpus"""
    format: str
    lang: str
    encoding: str = "utf8"
    path: Union[str, list]


class DocumentsConfig(BaseConfig):
    """Document processing task configuration"""
    input: DocumentsInputConfig
    process: TextProcessorConfig
    output: Union[bool, PathConfig]
    db: PathConfig


"""""""""""""""""
Topics & Queries
"""""""""""""""""


class TopicsInputConfig(BaseConfig):
    """Configuration for Topic input"""
    format: str
    lang: str
    encoding: str = "utf8"
    strip_non_digits: bool = False
    prefix: Union[bool, str] = "EN-"
    path: Union[str, list]


class TopicsConfig(BaseConfig):
    """Configuration for topics task"""
    input: TopicsInputConfig
    fields: str = "title"  # field1+field2 where field is title, desc, or narr
    output: Union[bool, PathConfig]


class QueriesInputConfig(BaseConfig):
    """Configuration for reading queries"""
    format: str = "json"
    encoding: str = "utf8"
    path: Union[str, list]


class QueriesConfig(BaseConfig):
    """Configuration for processing queries"""
    input: Optional[QueriesInputConfig]
    process: TextProcessorConfig
    output: Union[bool, PathConfig]


"""""""""""""""""
Index
"""""""""""""""""


class IndexInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    documents: PathConfig


class IndexConfig(BaseConfig):
    """Configuration for building an index"""
    input: Optional[IndexInputConfig]
    name: str
    output: PathConfig


"""""""""""""""""
Retrieve
"""""""""""""""""


class RetrieveIndexPathConfig(BaseConfig):
    path: dict  # index name: index path


class RetrieveInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    index: Union[PathConfig, RetrieveIndexPathConfig]
    queries: Optional[PathConfig]


class RetrieveConfig(BaseConfig):
    """Configuration for retrieval"""
    name: str
    number: int = 1000
    input: RetrieveInputConfig
    output: Union[bool, PathConfig]


"""""""""""""""""
Rerank
"""""""""""""""""


class RerankInputConfig(BaseConfig):
    """Configuration of optional rerank inputs"""
    db: PathConfig  # if running both stages, runner will copy this from documents config
    results: Optional[PathConfig]  # set if starting stage2 at reranking


class RerankConfig(BaseUncheckedConfig):
    """Configuration for the rerank task"""
    input: RerankInputConfig
    name: str
    script: Optional[str]  # for the shell reranker
    output: PathConfig


"""""""""""""""""
Score
"""""""""""""""""


class ScoreInputConfig(BaseConfig):
    """Qrels downstream configuration"""
    format: str = "trec"
    path: str


class ScoreConfig(BaseConfig):
    """Configuration for the scorer module"""
    metrics: list = ['map']
    input: ScoreInputConfig


"""""""""""""""""
Main
"""""""""""""""""


class StageConfig(BaseConfig):
    """Configuration for one of the stages"""
    mode: str = "streaming"
    batch_size: Optional[int]  # default is a single batch
    # start and stop are intended for parallel processing
    start: Optional[int]  # O-based index of start position in input (inclusive)
    stop: Optional[int]  # O-based index of stop position in input (exclusive)


class RunConfig(BaseConfig):
    """Configuration for a run of Patapsco"""
    name: str
    path: Optional[str]  # base path for run output by default created based on name
    parallel: Optional[str]  # MP or QSUB if running in parallel
    stage1: Union[bool, StageConfig] = StageConfig()
    stage2: Union[bool, StageConfig] = StageConfig()


class RunnerConfig(BaseConfig):
    """Configuration for the patapsco runner"""
    run: RunConfig
    documents: Optional[DocumentsConfig]
    index: Optional[IndexConfig]
    topics: Optional[TopicsConfig]
    queries: Optional[QueriesConfig]
    retrieve: Optional[RetrieveConfig]
    rerank: Optional[RerankConfig]
    score: Optional[ScoreConfig]