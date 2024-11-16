from abc import ABC, abstractmethod

class Transformer(ABC):
    @abstractmethod
    def transform(self, data):
        """Transform the data"""
        pass
