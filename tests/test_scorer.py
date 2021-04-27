
#from patapsco.util.additional_metrics import AdditionalMetrics


def test_ndcg_prime():
    qrels = "/exp/scale21/data/hc4/mini_scale_cmn_qrelsV0.1"
    system_output = "/exp/snair/scale21/zh/zh_scale/td_output/psq.trec"
    #ndcg_prime = AdditionalMetrics(qrels, system_output).ndcg_prime()


