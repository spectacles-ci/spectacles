from abc import ABC, abstractmethod
from spectacles.client import LookerClient


class Validator(ABC):  # pragma: no cover
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    @abstractmethod
    def validate(self):
        raise NotImplementedError
