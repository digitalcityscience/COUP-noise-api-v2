import hashlib
import json
import logging
from enum import Enum

logger = logging.getLogger(__name__)


def hash_dict(dict_) -> str:
    dict_str = json.dumps(dict_, sort_keys=True)
    return hashlib.md5(dict_str.encode()).hexdigest()


def enum_to_list(enum_class: Enum) -> list[str]:
    return [member.value for member in enum_class]


def load_json_file(path: str) -> dict:
    with open(path, "r") as f:
        return json.loads(f.read())
