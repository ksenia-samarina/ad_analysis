import abc
from abc import ABC

from pyflink.datastream import ProcessWindowFunction

class BaseTransformation(ProcessWindowFunction, ABC):
    @abc.abstractmethod
    def process(self, key, context, elements):
        raise NotImplementedError("Abstract method")
