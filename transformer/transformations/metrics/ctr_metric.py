import json

from prometheus_client import Gauge
from pyflink.datastream import ProcessWindowFunction

from transformer.transformations.base_transformation import BaseTransformation
from transformer.types.ad_types_enum import AdTypesEnum


class CTRMetric(BaseTransformation, ProcessWindowFunction):
    ctr_gauge = Gauge("ctr_metric", "CTR (clicks / views) value per ad_id", ["ad_id"])

    def process(self, key, context, elements):
        clicks, views = 0, 0
        for raw in elements:
            try:
                event = json.loads(raw)
                if event.get("ad_event") == AdTypesEnum.AD_WAS_CLICKED.value:
                    clicks += 1
                elif event.get("ad_event") == AdTypesEnum.AD_WAS_SHOWN.value:
                    views += 1
            except Exception as e:
                print(f"[WARN] Bad event: {raw}, err: {e}")

        ctr = clicks / views if views > 0 else 0.0
        ctr = min(ctr, 1.0) if ctr > 0.0 else ctr

        self._observe_ctr(key, ctr)

        yield key, ctr

    @staticmethod
    def _observe_ctr(ad_id: str, ctr: float):
        CTRMetric.ctr_gauge.labels(ad_id=str(ad_id)).set(float(ctr))
