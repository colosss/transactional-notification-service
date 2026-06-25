from pathlib import Path
from collections.abc import Mapping
from typing import Any
from src.core.domain.models import RenderedEmail

from jinja2 import Environment,

class JinjaTemplateRenderer:
    def __init__(self, templates_dir: Path):
        self._environment=