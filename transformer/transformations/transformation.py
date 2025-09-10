from pyflink.datastream import ProcessWindowFunction

from transformer.transformations.metrics import CTRMetric, ReachMetric
from transformer.transformations.base_transformation import BaseTransformation


class Transformation(BaseTransformation, ProcessWindowFunction):
    def process(self, key, context, elements):
        for metric_cls in [CTRMetric, ReachMetric]:
            metric = metric_cls()
            yield from metric.process(key, context, elements)