import json
from prometheus_client import Gauge
from pyflink.datastream import ProcessWindowFunction

from transformer.transformations.base_transformation import BaseTransformation
from transformer.types.ad_types_enum import AdTypesEnum


class ReachMetric(BaseTransformation, ProcessWindowFunction):
    reach_gauge = Gauge("reach_metric", "Unique reach (distinct users who saw ad) per ad_id", ["ad_id"])

    def process(self, key, context, elements):
        unique_users = set()

        for raw in elements:
            try:
                event = json.loads(raw)
                if event.get("ad_event") == AdTypesEnum.AD_WAS_SHOWN.value:
                    user_id = event.get("user_id")
                    if user_id is not None:
                        unique_users.add(user_id)
            except Exception as e:
                print(f"[WARN] Bad event: {raw}, err: {e}")

        reach = len(unique_users)

        self._observe_reach(key, reach)

        yield key, float(reach)

    @staticmethod
    def _observe_reach(ad_id: str, reach: int):
        ReachMetric.reach_gauge.labels(ad_id=str(ad_id)).set(float(reach))
