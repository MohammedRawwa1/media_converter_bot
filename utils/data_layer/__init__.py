# utils/data_layer/__init__.py
"""Laravel/Go-inspired data layer: fillable models and prepared queries."""

from .fillable import (
    FillableModel,
    GuardedFieldError,
    MassAssignmentError,
)
from .fillable import (
    ValidationError as FillableValidationError,
)
from .query_builder import (
    PreparedQuery,
    QueryBuilder,
    QueryError,
)
from .query_builder import (
    ValidationError as QueryValidationError,
)

__all__ = [
    "FillableModel",
    "GuardedFieldError",
    "MassAssignmentError",
    "FillableValidationError",
    "PreparedQuery",
    "QueryBuilder",
    "QueryError",
    "QueryValidationError",
]
