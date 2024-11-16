from abc import ABC, abstractmethod

class Processor(ABC):
    @abstractmethod
    def process(self, data):
        """Process the data"""
        pass
