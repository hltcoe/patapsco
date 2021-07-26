import collections
import logging
import sys

try:
    import pytrec_eval
except ImportError:
    pass

from .error import ConfigError
from .schema import ScoreInputConfig
from .util import ComponentFactory, GlobIterator
from .util.formats import parse_qrels

LOGGER = logging.getLogger(__name__)


class QrelsReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecQrelsReader',
        'msmarco': 'TrecQrelsReader',
    }
    config_class = ScoreInputConfig


class TrecQrelsReader:
    """Read TREC qrels files"""

    def __init__(self, config):
        self.path = config.path
        self.qrels_iter = GlobIterator(config.path, parse_qrels)

    def read(self):
        """
        Returns:
            dictionary of query_id -> {doc_id: relevance}
        """
        data = {}
        for qrels in self.qrels_iter:
            data = {**data, **qrels}
        return data


class Scorer:
    """Use pytrec_eval to calculate scores"""

    def __init__(self, qrels_config, metrics):
        """
        Args:
            qrels_config (PathConfig): Config for the qrels file or glob for multiple files.
            metrics (list): List of metrics names.
        """
        self.pytrec_eval_avail = 'pytrec_eval' in sys.modules
        if not self.pytrec_eval_avail:
            LOGGER.warning("pytrec_eval is not installed so no scoring is available")
            return
        self.metrics = self._preprocess_metrics(metrics)
        self.qrels = QrelsReaderFactory.create(qrels_config).read()
        self._validate_metrics(self.metrics)

    @staticmethod
    def _preprocess_metrics(metrics):
        """Replace @ with _ and standardize ndcg_prime"""
        metrics = [m.replace('@', '_') for m in metrics]
        return [m if m != "ndcg'" else "ndcg_prime" for m in metrics]

    @staticmethod
    def _validate_metrics(metrics):
        metrics = [m for m in metrics if m != "ndcg_prime"]
        try:
            pytrec_eval.RelevanceEvaluator({}, metrics)
        except ValueError as e:
            raise ConfigError(e)

    @staticmethod
    def _filter_dict(d, filter):
        for i in filter:
            d.pop(i, None)

    @staticmethod
    def _add_dict(d, misses):
        for i in misses:
            d[i] = {}

    def score(self, results_path, scores_path):
        """Calculate scores at the end of the run.

        Args:
            results_path (Path): Path to results of a run.
            scores_path (Path): Path to write scores.
        """
        if not self.pytrec_eval_avail:
            return

        with open(results_path, 'r') as fp:
            system_output = pytrec_eval.parse_run(fp)
        remove_from_run = set(system_output.keys()) - set(self.qrels.keys())
        if remove_from_run:
            LOGGER.warning(f'Omitting {len(remove_from_run)} topics in the run that are not in the qrels')
            self._filter_dict(system_output, remove_from_run)
        missing_queries = set(self.qrels.keys()) - set(system_output.keys())
        if missing_queries:
            LOGGER.warning(f"Missing queries: {', '.join(missing_queries)}")
            self._add_dict(system_output, missing_queries)
        measures = {s for s in self.metrics}
        ndcg_prime_results = {}
        if "ndcg_prime" in measures:
            ndcg_prime_results = self._calc_ndcg_prime(system_output)
            measures.discard("ndcg_prime")
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, measures)
        scores = evaluator.evaluate(system_output)
        if ndcg_prime_results:
            for query in scores.keys():
                scores[query].update(ndcg_prime_results[query])
        if scores:
            mean_scores = {}
            for key in sorted(self.metrics):
                mean_scores[key] = sum(data[key] for data in scores.values()) / len(scores)
            scores_string = ", ".join(f"{m}: {s:.3f}" for m, s in mean_scores.items())
            LOGGER.info(f"Average scores over {len(scores.keys())} queries: {scores_string}")
            self._write_scores(scores, scores_path)

    def _calc_ndcg_prime(self, system_output):
        """Calculate nDCG'

        For every query, remove document ids that do not belong to the set of
        judged documents for that query, and run nDCG over the modified output.
        """
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, {'ndcg'})
        modified_run = collections.defaultdict(dict)
        for query_id in system_output:
            for doc_id in system_output[query_id]:
                if doc_id in self.qrels[query_id].keys():
                    modified_run[query_id][doc_id] = system_output[query_id][doc_id]
        ndcg_prime_scores = evaluator.evaluate(modified_run)
        ndcg_scores = evaluator.evaluate(system_output)
        combined_scores = {}
        for query_id in system_output:
            if query_id in ndcg_prime_scores.keys():
                combined_scores[query_id] = {"ndcg_prime": ndcg_prime_scores[query_id]['ndcg']}
            else:
                # if no ndcg_prime result, ndcg_prime == ndcg
                combined_scores[query_id] = {"ndcg_prime": ndcg_scores[query_id]['ndcg']}
        return combined_scores

    def _write_scores(self, scores, scores_path):
        with open(scores_path, 'w') as fp:
            for q, results_dict in sorted(scores.items()):
                for measure, value in sorted(results_dict.items()):
                    print('{:25s}\t{}\t{:.4f}'.format(measure, q, value), file=fp)

            for measure in sorted(self.metrics):
                query_scores = [results_dict[measure] for results_dict in scores.values()]
                agg = pytrec_eval.compute_aggregated_measure(measure, query_scores)
                print('{:25s}\t{}\t{:.4f}'.format(measure, 'all', agg), file=fp)
