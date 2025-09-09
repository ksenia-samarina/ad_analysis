import random
from typing import Tuple

from generator.registry.ad_types_enum import AdTypesEnum
from generator.registry.ad_type_registry import AbstractGenerator


class AdTypeShownGenerator(AbstractGenerator):
    _generator = AdTypesEnum.AD_WAS_SHOWN

    def execute(self) -> Tuple[int, str, int]:
        ad_id = random.randint(1, 10)
        user_id = random.randint(0, 1_000_000)
        return ad_id, self._generator.value, user_id


class AdTypeClickedGenerator(AbstractGenerator):
    _generator = AdTypesEnum.AD_WAS_CLICKED

    def execute(self) -> Tuple[int, str, int]:
        ad_id = random.randint(1, 10)
        user_id = random.randint(0, 1_000_000)
        return ad_id, self._generator.value, user_id