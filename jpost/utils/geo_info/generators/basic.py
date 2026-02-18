from abc import ABC, abstractmethod


class AbstractGeoInfoGenerator(ABC):
       
    @abstractmethod
    def generate_params(self) -> dict[str, str]:
        pass

    @abstractmethod
    def get_best_result(self) -> dict[str, str]:
        pass

    @abstractmethod
    def parse_result(self) -> dict[str, str]:
        pass
