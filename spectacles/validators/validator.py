from typing import List
from abc import ABC, abstractmethod
from spectacles.client import LookerClient
from spectacles.exceptions import ValidationError
from spectacles.lookml import Project


class Validator(ABC):
    """Defines abstract base interface for validators.

    Not intended to be used directly, only inherited.

    Attributes:
        client: Looker API client.

    """

    def __init__(self, client: LookerClient):
        self.client = client

    @abstractmethod
    def validate(self, project: Project) -> List[ValidationError]:
        raise NotImplementedError
