import collections
import logging
import pytrec_eval

from .pipeline import Task
from .schema import ScoreInputConfig
from .util import ComponentFactory, GlobIterator, trec

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
        self.qrels_iter = GlobIterator(config.path, trec.parse_qrels)

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
    """Scorer module"""

    def __init__(self, run_path, config, qrels):
        """
        Args:
            run_path (str): Root directory of the run.
            config (ScoreConfig)
            qrels (dict): qrels dictionary
        """
        super().__init__(run_path, base='')
        self.config = config
        self.config.metrics = [m.replace('@', '_').capitalize()
                               if m[:2] == 'p@' else m.replace('@', '_')
                               for m in self.config.metrics]
        self.qrels = qrels
        self.run = collections.defaultdict(dict)

    def process(self, results):
        """ Accumulate the results and calculate scores at end

        Args:
            results (Results): Results for a query

        Return:
            Results
        """
        for result in results.results:
            self.run[results.query.id][result.doc_id] = result.score
        return results

    def end(self):
        measures = {s for s in self.config.metrics}
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, measures)
        res = evaluator.evaluate(self.run)
        avgs = collections.defaultdict(list)
        for q, results_dict in res.items():
            for metric, score in results_dict.items():
                avgs[metric].append(score)
        averages = {metric: sum(scores) / len(scores) for
                    metric, scores in avgs.items()}
        LOGGER.info(f"Average scores over {len(res.keys())} queries: {averages}")

    def calc_ndcg_prime(self):
        """ Calculate nDCG': for every query, remove document ids that do not
        belong to the set of judged documents for that query, and run nDCG
        over the modified run
        """
        evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, {'ndcg'})
        modified_run = collections.defaultdict(dict)

        for query_id in self.run:
            for doc_id in self.run[query_id]:
                if doc_id in self.qrels[query_id].keys():
                    modified_run[query_id][doc_id] = self.run[query_id][doc_id]
        rename = evaluator.evaluate(modified_run)
        res = {}
        for elt in rename.items():
            res[elt[0]] = {'ndcg_prime': elt[1]['ndcg']}
        for q, results_dict in res.items():
            LOGGER.info(f"{q} = {results_dict}")
        return res
