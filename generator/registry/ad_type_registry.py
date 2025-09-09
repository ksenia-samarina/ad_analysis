from abc import ABC, abstractmethod
from typing import Type, Dict, Any, final, Tuple

from generator.registry.ad_types_enum import AdTypesEnum


class AdTypeRegistry(ABC):
    _registry: Dict[str, Type["AbstractGenerator"]] = {}

    def __init_subclass__(cls: Type["AdTypeRegistry"], **kwargs: Dict[str, Any]) -> None:
        super().__init_subclass__(**kwargs)
        generator = cls.get_generator()
        print(generator, cls)
        if generator is not None and issubclass(cls, AbstractGenerator):
            AdTypeRegistry.get_registry()[generator.value] = cls

    @classmethod
    @final
    def get_registry(cls: Type["AdTypeRegistry"]) -> Dict[str, Type["AbstractGenerator"]]:
        return cls._registry

    @classmethod
    @final
    def get_generator_by_name(cls: Type["AdTypeRegistry"], generator_name: str) -> Type["AbstractGenerator"]:
        return cls._registry[generator_name]

    @classmethod
    @abstractmethod
    def get_generator(cls: Type["AdTypeRegistry"]) -> AdTypesEnum | None:
        """"""


class AbstractGenerator(AdTypeRegistry, ABC):
    _generator: AdTypesEnum | None = None

    @classmethod
    def get_generator(cls: Type["AbstractGenerator"]) -> AdTypesEnum | None:
        return cls._generator

    @abstractmethod
    def execute(self) -> Tuple[int, str, int]:
        """"""
