from dataclasses import dataclass, asdict
from typing import List


@dataclass
class ModuleDetail:
    name: str
    description: str
    config: List["ModuleConfiguration"] = None
    url: str = None

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ModuleConfiguration:
    name: str
    type: str
    desc: str
