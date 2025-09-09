from enum import Enum
from typing import Type, List


class AdTypesEnum(Enum):
    AD_WAS_SHOWN = "AD_WAS_SHOWN"
    AD_WAS_CLICKED = "AD_WAS_CLICKED"

    @classmethod
    def values(cls: Type["AdTypesEnum"]) -> List[str]:
        return [e.value for e in cls]