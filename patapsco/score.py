import collections
import logging
import pytrec_eval

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
        self.metrics = [m.replace('@', '_').capitalize()
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
        if set(self.run.keys()) - set(self.qrels.keys()):
            LOGGER.warning('There are queries in the run that are not in the qrels')
        measures = {s for s in self.metrics}
        ndcg_prime_results = {}
        if "ndcg'" in measures or "ndcg_prime" in measures:
            ndcg_prime_results = self.calc_ndcg_prime()
            measures.discard("ndcg'")
            measures.discard("ndcg_prime")
        try:
            evaluator = pytrec_eval.RelevanceEvaluator(self.qrels, measures)
        except ValueError as e:
            LOGGER.warning(e)
            raise ValueError(e)
        else:
            scores = evaluator.evaluate(self.run)
            if ndcg_prime_results:
                for q, results_dict in scores.items():
                    scores[q].update(ndcg_prime_results[q])
            if scores:
                mean_scores = {}
                metrics = list(list(scores.values())[0].keys())
                for key in metrics:
                    mean_scores[key] = sum(data[key] for data in scores.values()) / len(scores)
                scores_string = ", ".join(f"{m}: {s:.3f}" for m, s in mean_scores.items())
                LOGGER.info(f"Average scores over {len(scores.keys())} queries: {scores_string}")
                scores_path = self.run_path / 'scores.txt'

                with open(scores_path, 'w') as fp:
                    for q, results_dict in sorted(scores.items()):
                        for measure, value in sorted(results_dict.items()):
                            print('{:25s}{:8s}{:.4f}'.format(measure, q, value),
                                  file=fp)

                    for measure in sorted(results_dict.keys()):
                        print('{:25s}{:8s}{:.4f}'.format(measure, 'all',
                              pytrec_eval.compute_aggregated_measure(
                                  measure, [results_dict[measure]
                                            for results_dict in scores.values()])), file=fp)

            elif self.run and self.qrels:
                LOGGER.warning("There is a likely mismatch between query ids and qrels")

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
            res[elt[0]] = {"ndcg'": elt[1]['ndcg']}
        return res
