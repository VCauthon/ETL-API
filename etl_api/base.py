from dataclasses import dataclass, asdict
from typing import List


@dataclass
class ModuleDetail:
    name: str
    description: str
    config: List['ModuleConfiguration'] = None
    api_entry_point: bool = False
    url: str = None

    def __post_init__(self):
        if self.api_entry_point:
            self.url = f"/api/{self.name.lower()}"

    def to_dict(self):
        dict_version = asdict(self)
        dict_version.pop("api_entry_point")
        return {k: v for k, v in dict_version.items() if v is not None}
        


@dataclass
class ModuleConfiguration:
    name: str
    type: str
    desc: str
