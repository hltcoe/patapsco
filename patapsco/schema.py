import enum

from .config import BaseConfig, SectionConfig, UncheckedSectionConfig, Optional, Union


class PipelineMode(str, enum.Enum):
    STREAMING = 'streaming'
    BATCH = 'batch'


class Tasks(str, enum.Enum):
    """Tasks that make up the system pipelines"""
    DOCUMENTS = 'documents'
    DATABASE = 'database'
    INDEX = 'index'
    TOPICS = 'topics'
    QUERIES = 'queries'
    RETRIEVE = 'retrieve'
    RERANK = 'rerank'


class PathConfig(BaseConfig):
    """Simple config with only a path variable"""
    path: str


# """""""""""""""""
# Text Processing
# """""""""""""""""


class NormalizationConfig(BaseConfig):
    report: bool = False  # save a report of normalization changes
    lowercase: bool = True


class TextProcessorConfig(SectionConfig):
    """Configuration for the text processing"""
    model_path: Optional[str]  # path to spacy or stanza model directory
    normalize: NormalizationConfig = NormalizationConfig()
    tokenize: str
    stopwords: Union[bool, str] = "lucene"
    stem: Union[bool, str] = False
    strict_check: bool = True  # check whether the processing is the same for documents and queries


# """""""""""""""""
# Documents
# """""""""""""""""


class DocumentsInputConfig(BaseConfig):
    """Configuration for the document corpus"""
    format: str
    lang: str
    encoding: str = "utf8"
    path: Union[str, list]


class DocumentsConfig(SectionConfig):
    """Document processing task configuration"""
    input: DocumentsInputConfig
    process: TextProcessorConfig
    output: Union[bool, str] = False


# """""""""""""""""
# Database
# """""""""""""""""


class DatabaseConfig(SectionConfig):
    name: str = 'sqlite'
    output: Union[bool, str] = True


# """""""""""""""""
# Topics & Queries
# """""""""""""""""


class TopicsInputConfig(BaseConfig):
    """Configuration for Topic input"""
    format: str
    lang: str
    encoding: str = "utf8"
    filter_lang: Optional[str]  # language code - filter out topics that do not have this language in lang_supported
    strip_non_digits: bool = False
    prefix: Union[bool, str] = False  # use "EN-" or similar for CLEF sgml topics
    path: Union[str, list]


class TopicsConfig(SectionConfig):
    """Configuration for topics task"""
    input: TopicsInputConfig
    fields: str = "title"  # field1+field2 where field is title, desc, or narr
    output: Union[bool, str] = False


class QueriesInputConfig(BaseConfig):
    """Configuration for reading queries"""
    format: str = "json"
    encoding: str = "utf8"
    path: Union[str, list]


class PSQConfig(BaseConfig):
    """Probabilistic Structured Query configuration"""
    path: str  # path to a translation table
    threshold: float = 0.97  # cumulative probability threshold
    lang: str  # language code of documents
    # Text processing configuration after PSQ projection
    normalize: NormalizationConfig = NormalizationConfig()
    stopwords: Union[bool, str] = "lucene"
    stem: Union[bool, str] = False


class QueriesConfig(SectionConfig):
    """Configuration for processing queries"""
    input: Optional[QueriesInputConfig]
    process: TextProcessorConfig
    psq: Optional[PSQConfig]
    parse: bool = False  # parse with Lucene query parser with support for boolean operators and term weighting
    output: Union[bool, str] = True


# """""""""""""""""
# Index
# """""""""""""""""


class IndexInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    documents: PathConfig


class IndexConfig(SectionConfig):
    """Configuration for building an index"""
    input: Optional[IndexInputConfig]
    name: str
    output: Union[bool, str] = True


# """""""""""""""""
# Retrieve
# """""""""""""""""


class RetrieveInputConfig(BaseConfig):
    """Configuration of optional retrieval inputs"""
    index: Union[None, PathConfig]
    queries: Optional[PathConfig]


class RetrieveConfig(SectionConfig):
    """Configuration for retrieval"""
    name: str  # bm25 or qld
    number: int = 1000
    input: Optional[RetrieveInputConfig]
    output: Union[bool, str] = True
    log_explanations: bool = False
    log_explanations_cutoff: int = 10

    parse: bool = False  # set to true if using Lucene classic query parser (won't support RM3)

    # Parameters for retrieval approaches
    # bm25
    k1: float = 0.9
    b: float = 0.4
    # qld - Query likelihood with Dirichlet smoothing
    mu: int = 1000
    # probabilistic structured query
    psq: bool = False
    # rm3 query expansion
    rm3: bool = False
    fb_terms = 10
    fb_docs = 10
    original_query_weight = float(0.5)
    rm3_logging: bool = False  # log expanded queries to rm3.log


# """""""""""""""""
# Rerank
# """""""""""""""""


class RerankInputConfig(BaseConfig):
    """Configuration of optional rerank inputs"""
    database: Optional[PathConfig]  # if running both stages, runner will copy this from documents config
    results: Optional[PathConfig]  # set if starting stage2 at reranking


class RerankConfig(UncheckedSectionConfig):
    """Configuration for the rerank task"""
    input: Optional[RerankInputConfig]
    name: str
    script: Optional[str]  # for the shell reranker
    output: Union[bool, str] = False


# """""""""""""""""
# Score
# """""""""""""""""


class ScoreInputConfig(BaseConfig):
    """Qrels downstream configuration"""
    format: str = "trec"
    path: str  # path to qrels file or glob to match multiple files


class ScoreConfig(SectionConfig):
    """Configuration for the scorer module"""
    metrics: list = ['ndcg_prime', 'ndcg', 'map', 'recall_100', 'recall_1000']
    input: ScoreInputConfig


# """""""""""""""""
# Main
# """""""""""""""""


class StageConfig(BaseConfig):
    """Configuration for one of the stages"""
    mode: str = "streaming"  # streaming or batch
    batch_size: Optional[int]  # for batch, the default is a single batch
    num_jobs: int = 1  # number of parallel jobs
    progress_interval: Optional[int]  # how often should progress be logged
    # start and stop are intended for parallel processing
    start: Optional[int]  # O-based index of start position in input (inclusive)
    stop: Optional[int]  # O-based index of stop position in input (exclusive)


class ParallelConfig(BaseConfig):
    name: str  # mp, qsub, or sbatch
    queue: Optional[str] = "all.q"  # used for qsub jobs
    email: Optional[str]  # email address for job completion notifications
    resources: str = "h_rt=12:00:00"  # default to 12 hours as an upper limit (this is qsub format)
    code: Optional[str]  # extra lines to add to bash scripts


class RunConfig(SectionConfig):
    """Configuration for a run of Patapsco"""
    name: str
    path: Optional[str]  # base path for run output by default created based on name
    results: str = "results.txt"  # default results filename
    parallel: Optional[ParallelConfig]  # configure for a parallel job
    stage1: Union[bool, StageConfig] = StageConfig()
    stage2: Union[bool, StageConfig] = StageConfig()


class RunnerConfig(BaseConfig):
    """Configuration for the patapsco runner"""
    run: RunConfig
    text: Optional[TextProcessorConfig]
    documents: Optional[DocumentsConfig]
    database: Optional[DatabaseConfig]
    index: Optional[IndexConfig]
    topics: Optional[TopicsConfig]
    queries: Optional[QueriesConfig]
    retrieve: Optional[RetrieveConfig]
    rerank: Optional[RerankConfig]
    score: Optional[ScoreConfig]
