# utils/data_layer/fillable.py
"""
Laravel-inspired $fillable / $guarded mass-assignment protection.

Usage:
    class UserModel(FillableModel):
        fillable = {"username", "email", "avatar_url"}
        guarded = {"is_admin", "role"}

        collection_name = "users"

    model = UserModel(db)
    model.create({"username": "john", "is_admin": True})  # is_admin silently stripped
    model.update(user_id, {"role": "admin"})               # raises GuardedFieldError
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class GuardedFieldError(ValueError):
    """Raised when attempting to mass-assign a guarded field."""


class MassAssignmentError(ValueError):
    """Raised when ALL fields are guarded (mass assignment completely disabled)."""


class FillableModel:
    """Base class for secure mass-assignment handling.

    Subclasses define:
        fillable: Set of field names allowed for mass assignment.
                  If empty and guarded is also empty, all fields are fillable.
        guarded:  Set of field names NEVER allowed for mass assignment.
                  Defaults to {"_id"} to protect the primary key.

    The model attribute 'guarded' acts as a deny-list: any field in this
    set is silently dropped or raises an error if explicitly set.

    If fillable is non-empty, it acts as an allow-list: ONLY fields in
    this set are permitted during mass assignment.  Fields not in fillable
    are silently stripped.

    If both fillable and guarded are empty, all fields are allowed (not
    recommended for production).
    """

    # Override in subclasses
    fillable: set[str] = set()
    guarded: set[str] = {"_id"}
    collection_name: str = ""  # Mongo collection name

    # ── Fillable / Guarded logic ──────────────────────────────────────

    @classmethod
    def _get_fillable_fields(cls) -> set[str]:
        """Return the effective set of fillable fields."""
        return cls.fillable

    @classmethod
    def _get_guarded_fields(cls) -> set[str]:
        """Return the effective set of guarded fields."""
        return cls.guarded

    @classmethod
    def filter_fillable(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Strip non-fillable and guarded fields from a dict.

        If `fillable` is empty and `guarded` only contains the default
        '_id', all fields are allowed (loose mode).  Otherwise only
        fields explicitly listed in `fillable` and not in `guarded`
        are retained.

        This prevents mass-assignment attacks where an attacker sends
        extra fields like `is_admin=True` in a create/update payload.
        """
        fillable = cls._get_fillable_fields()
        guarded = cls._get_guarded_fields()

        if not fillable and guarded == {"_id"}:
            # Loose mode: only strip the default guarded field
            return {k: v for k, v in data.items() if k not in guarded}

        if fillable:
            # Allow-list mode: only keep fields in fillable AND not guarded
            allowed = fillable - guarded
            return {k: v for k, v in data.items() if k in allowed}

        # Deny-list mode: strip guarded fields
        return {k: v for k, v in data.items() if k not in guarded}

    @classmethod
    def strip_guarded(cls, data: dict[str, Any], strict: bool = False) -> dict[str, Any]:
        """Remove guarded fields from data.

        In strict mode, raises GuardedFieldError if any guarded field
        is present in the data.
        """
        guarded = cls._get_guarded_fields()
        if strict:
            for key in data:
                if key in guarded:
                    raise GuardedFieldError(
                        f"Field '{key}' is guarded and cannot be set via mass assignment"
                    )
        return {k: v for k, v in data.items() if k not in guarded}

    # ── Projection helpers ────────────────────────────────────────────

    @classmethod
    def safe_projection(cls, fields: set[str] | None = None) -> dict[str, int]:
        """Build a MongoDB projection dict that only returns fillable fields.

        If `fields` is given, it's intersected with fillable.  Otherwise
        all fillable fields are included.  Always excludes `_id` unless
        it's explicitly requested.
        """
        fillable = cls._get_fillable_fields()
        if fields is not None:
            allowed = fillable & fields if fillable else fields
        else:
            allowed = fillable if fillable else set()

        projection = {}
        for f in allowed:
            projection[f] = 1
        if fillable and "_id" not in allowed:
            projection["_id"] = 0
        return projection

    # ── Query sanitisation helpers ────────────────────────────────────

    @classmethod
    def sanitize_regex(cls, user_input: str) -> str:
        """Escape user input for use in MongoDB $regex queries.

        Prevents regex injection attacks where a user could craft a
        pattern that causes catastrophic backtracking or matches
        unintended documents.
        """
        import re
        return re.escape(user_input)

    @classmethod
    def validate_sort_field(cls, field: str) -> str:
        """Validate that a sort field is a known fillable field.

        Prevents MongoDB $sort injection via field names containing
        special operators like {$meta: "textScore"}.
        """
        fillable = cls._get_fillable_fields()
        if fillable and field not in fillable:
            raise ValidationError(
                f"Sort field '{field}' is not in fillable fields: {fillable}"
            )
        if field.startswith("$"):
            raise ValidationError(f"Sort field cannot start with '$': {field}")
        return field

    @classmethod
    def sanitize_update_operators(cls, update: dict[str, Any]) -> dict[str, Any]:
        """Ensure update dict only uses safe MongoDB operators.

        Only allows $set, $inc, $push, $pull, $unset.  Prohibits
        $where, $expr, $accumulator, $function to prevent injection.
        """
        allowed_ops = {"$set", "$inc", "$push", "$pull", "$unset", "$addToSet", "$each", "$position"}
        for op in update:
            if op.startswith("$") and op not in allowed_ops:
                raise ValidationError(f"Update operator '{op}' is not allowed")
        return update

    @classmethod
    def sanitize_filter(cls, filter_dict: dict[str, Any]) -> dict[str, Any]:
        """Sanitize a MongoDB filter dict to prevent NoSQL injection.

        Strips dangerous operators like $where, $expr, $accumulator,
        $function, $near, $geoNear that could be used for injection.
        Also validates that field names don't start with $.
        """
        dangerous_ops = {"$where", "$expr", "$accumulator", "$function", "$near", "$geoNear"}
        sanitized = {}
        for key, value in filter_dict.items():
            if key.startswith("$") and key in dangerous_ops:
                raise ValidationError(f"Dangerous query operator '{key}' is not allowed")
            if isinstance(value, dict):
                # Recursively sanitize nested operators
                sanitized[key] = cls.sanitize_filter(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    cls.sanitize_filter(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                sanitized[key] = value
        return sanitized


class ValidationError(ValueError):
    """Raised when data fails validation rules."""
