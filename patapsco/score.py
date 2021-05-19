import collections
import logging
import pytrec_eval

from .error import ConfigError
from .pipeline import Task
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


class Scorer(Task):
    """Use pytrec_eval to calculate scores"""

    def __init__(self, run_path, config, qrels):
        """
        Args:
            run_path (str): Root directory of the run.
            config (ScoreConfig)
            qrels (dict): qrels dictionary
        """
        super().__init__(run_path, base='')
        self.config = config
        self.metrics = self._preprocess_metrics(self.config.metrics)
        self.qrels = qrels
        self.run = collections.defaultdict(dict)
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

    def process(self, results):
        """Accumulate the results and calculate scores at end

        Args:
            results (Results): Results for a query

        Return:
            Results
        """
        for result in results.results:
            self.run[results.query.id][result.doc_id] = result.score
        return results

    def end(self):
        """Calculate scores at the end of the run"""
        if set(self.run.keys()) - set(self.qrels.keys()):
            LOGGER.warning('There are queries in the run that are not in the qrels')
        measures = {s for s in self.metrics}
        ndcg_prime_results = {}
        if "ndcg_prime" in measures:
            ndcg_prime_results = self._calc_ndcg_prime()
            measures.discard("ndcg_prime")
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, measures)
        scores = evaluator.evaluate(self.run)
        if ndcg_prime_results:
            for query in scores.keys():
                scores[query].update(ndcg_prime_results[query])
        if scores:
            mean_scores = {}
            for key in sorted(self.metrics):
                mean_scores[key] = sum(data[key] for data in scores.values()) / len(scores)
            scores_string = ", ".join(f"{m}: {s:.3f}" for m, s in mean_scores.items())
            LOGGER.info(f"Average scores over {len(scores.keys())} queries: {scores_string}")
            self._write_scores(scores)

    def _calc_ndcg_prime(self):
        """Calculate nDCG'

        For every query, remove document ids that do not belong to the set of
        judged documents for that query, and run nDCG over the modified output.
        """
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, {'ndcg'})
        modified_run = collections.defaultdict(dict)
        for query_id in self.run:
            for doc_id in self.run[query_id]:
                if doc_id in self.qrels[query_id].keys():
                    modified_run[query_id][doc_id] = self.run[query_id][doc_id]
        ndcg_scores = evaluator.evaluate(modified_run)
        return {query: {"ndcg_prime": scores["ndcg"]} for query, scores in ndcg_scores.items()}

    def _write_scores(self, scores):
        scores_path = self.run_path / 'scores.txt'
        with open(scores_path, 'w') as fp:
            for q, results_dict in sorted(scores.items()):
                for measure, value in sorted(results_dict.items()):
                    print('{:25s}{:8s}{:.4f}'.format(measure, q, value), file=fp)

            for measure in sorted(self.metrics):
                query_scores = [results_dict[measure] for results_dict in scores.values()]
                agg = pytrec_eval.compute_aggregated_measure(measure, query_scores)
                print('{:25s}{:8s}{:.4f}'.format(measure, 'all', agg), file=fp)
