from collections.abc import Mapping
from typing import Any, Protocol

from src.core.domain.models import RenderedEmail

class TemplateRenderer(Protocol):
    def render(
        self,
        template_code: str,
        context: Mapping[str, Any],
    )->RenderedEmail: ...