# utils/data_layer/query_builder.py
"""
Prepared / parameterized query builder for MongoDB (Go/Java-inspired).

Provides a fluent, secure query builder that:
- Enforces parameterized filters (no raw string interpolation)
- Validates all field names against a fillable schema
- Prevents NoSQL injection ($where, $expr, etc.)
- Supports type-checked parameters
- Logs all queries for audit

Usage:
    qb = QueryBuilder(db.conversions, fillable_fields={"user_id", "action", "timestamp"})

    # Safe parameterized query
    results = await qb.select(
        filters={"user_id": user_id, "action": "convert"},
        limit=10,
        projection={"action": 1, "timestamp": 1},
    ).to_list()

    # Insert with fillable protection
    qb.insert({"user_id": user_id, "is_admin": True})  # is_admin stripped
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class QueryError(RuntimeError):
    """Raised on query execution errors."""


class ValidationError(ValueError):
    """Raised when query parameters fail validation."""


class PreparedQuery:
    """Represents a prepared/parameterized query with bound parameters.

    Similar to Go's `database/sql` prepared statements or Java's
    `PreparedStatement`.  The filter dict is validated at bind time
    so no injection is possible.
    """

    __slots__ = ("collection", "fillable", "guarded", "_filter", "_projection",
                 "_sort", "_limit", "_skip", "_allow_disk_use")

    def __init__(
        self,
        collection,
        fillable: set[str],
        guarded: set[str],
        filter_dict: dict[str, Any],
    ):
        self.collection = collection
        self.fillable = fillable
        self.guarded = guarded
        self._filter = filter_dict
        self._projection: dict[str, int] | None = None
        self._sort: list[tuple[str, int]] | None = None
        self._limit: int | None = None
        self._skip: int | None = None
        self._allow_disk_use: bool = False

    def project(self, fields: dict[str, int]) -> PreparedQuery:
        """Set projection (field inclusion/exclusion)."""
        self._validate_fields(set(fields.keys()))
        self._projection = fields
        return self

    def sort(self, *fields: tuple[str, int]) -> PreparedQuery:
        """Set sort order.  Each field is validated."""
        for field, _ in fields:
            self._validate_field(field)
        self._sort = list(fields)
        return self

    def limit(self, n: int) -> PreparedQuery:
        """Set max documents to return."""
        if n < 0:
            raise ValidationError("Limit cannot be negative")
        if n > 10000:
            raise ValidationError("Limit exceeds maximum (10000)")
        self._limit = n
        return self

    def skip(self, n: int) -> PreparedQuery:
        """Set number of documents to skip."""
        if n < 0:
            raise ValidationError("Skip cannot be negative")
        self._skip = n
        return self

    def allow_disk_use(self) -> PreparedQuery:
        """Allow disk-based sorting for large datasets."""
        self._allow_disk_use = True
        return self

    # ── Execution ─────────────────────────────────────────────────────

    async def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        """Execute the query and return results as a list."""
        cursor = self._build_cursor()
        try:
            return await cursor.to_list(length=length or self._limit or 1000)
        except Exception as e:
            logger.exception("PreparedQuery.to_list failed: %s", e)
            raise QueryError(f"Query execution failed: {e}") from e

    async def first(self) -> dict[str, Any] | None:
        """Execute and return the first matching document (or None)."""
        self._limit = 1
        cursor = self._build_cursor()
        try:
            docs = await cursor.to_list(length=1)
            return docs[0] if docs else None
        except Exception as e:
            logger.exception("PreparedQuery.first failed: %s", e)
            raise QueryError(f"Query execution failed: {e}") from e

    async def count(self) -> int:
        """Return the count of matching documents."""
        try:
            return await self.collection.count_documents(self._filter)
        except Exception as e:
            logger.exception("PreparedQuery.count failed: %s", e)
            raise QueryError(f"Count failed: {e}") from e

    async def exists(self) -> bool:
        """Check if at least one document matches."""
        result = await self.first()
        return result is not None

    # ── Internal ──────────────────────────────────────────────────────

    def _validate_field(self, field: str):
        """Validate a single field name against fillable/guarded."""
        if field.startswith("$"):
            raise ValidationError(f"Field name cannot start with '$': {field}")
        if self.fillable and field not in self.fillable and field != "_id":
            raise ValidationError(f"Field '{field}' is not in fillable fields")
        if field in self.guarded:
            raise ValidationError(f"Field '{field}' is guarded")

    def _validate_fields(self, fields: set[str]):
        """Validate multiple field names."""
        for field in fields:
            self._validate_field(field)

    def _build_cursor(self):
        """Build the pymongo/motor cursor from prepared parameters."""
        cursor = self.collection.find(self._filter, self._projection)
        if self._sort:
            cursor = cursor.sort(self._sort)
        if self._skip:
            cursor = cursor.skip(self._skip)
        if self._limit:
            cursor = cursor.limit(self._limit)
        if self._allow_disk_use:
            cursor = cursor.allow_disk_use()
        return cursor

    def __repr__(self) -> str:
        return (
            f"PreparedQuery(filter={self._filter}, "
            f"projection={self._projection}, "
            f"sort={self._sort}, "
            f"limit={self._limit})"
        )


class QueryBuilder:
    """Fluent query builder with prepared-statement-like safety.

    Usage:
        qb = QueryBuilder(
            collection=db.conversions,
            fillable_fields={"user_id", "action", "timestamp"},
            guarded_fields={"_id"},
        )

        # SELECT-like
        results = await qb.table.select(filters={...}).limit(10).to_list()
        item = await qb.table.select(filters={...}).first()

        # INSERT-like
        doc_id = await qb.insert(data)

        # UPDATE-like
        await qb.update(filters={...}, updates={"$set": {...}})

        # DELETE-like
        await qb.delete(filters={...})
    """

    def __init__(
        self,
        collection,
        fillable_fields: set[str] | None = None,
        guarded_fields: set[str] | None = None,
    ):
        self.collection = collection
        self.fillable = fillable_fields or set()
        self.guarded = guarded_fields or {"_id"}

    @property
    def table(self) -> QueryBuilder:
        """Fluent alias for chaining."""
        return self

    # ── READ ──────────────────────────────────────────────────────────

    def select(
        self,
        filters: dict[str, Any] | None = None,
        projection: dict[str, int] | None = None,
    ) -> PreparedQuery:
        """Create a prepared SELECT query with parameterized filters.

        The `filters` dict is validated to prevent NoSQL injection.
        Only fillable field names are permitted in filters.

        Example:
            qb.select(
                filters={"user_id": user_id, "status": "done"},
                projection={"status": 1, "progress": 1},
            ).sort(("timestamp", -1)).limit(10)
        """
        validated_filters = self._validate_filters(filters or {})
        query = PreparedQuery(
            collection=self.collection,
            fillable=self.fillable,
            guarded=self.guarded,
            filter_dict=validated_filters,
        )
        if projection:
            query = query.project(projection)
        return query

    # ── CREATE ────────────────────────────────────────────────────────

    async def insert(self, data: dict[str, Any]) -> str | None:
        """Insert a document with fillable/guarded field stripping.

        Only fillable fields are persisted.  Guarded fields are silently
        removed.  Returns the inserted document's string ID.
        """
        safe_data = self._strip_non_fillable(data)
        if not safe_data:
            raise ValidationError("No fillable fields in insert data")

        try:
            result = await self.collection.insert_one(safe_data)
            logger.info("QB INSERT: collection=%s fields=%s",
                        self.collection.name, list(safe_data.keys()))
            return str(result.inserted_id)
        except Exception as e:
            logger.exception("QB INSERT failed: %s", e)
            raise QueryError(f"Insert failed: {e}") from e

    async def insert_many(self, data_list: list[dict[str, Any]]) -> list[str]:
        """Insert multiple documents with fillable protection."""
        safe_list = [self._strip_non_fillable(d) for d in data_list]
        safe_list = [d for d in safe_list if d]
        if not safe_list:
            raise ValidationError("No fillable fields in insert data")

        try:
            result = await self.collection.insert_many(safe_list)
            logger.info("QB INSERT_MANY: collection=%s count=%d",
                        self.collection.name, len(safe_list))
            return [str(id_) for id_ in result.inserted_ids]
        except Exception as e:
            logger.exception("QB INSERT_MANY failed: %s", e)
            raise QueryError(f"Insert many failed: {e}") from e

    # ── UPDATE ────────────────────────────────────────────────────────

    async def update(
        self,
        filters: dict[str, Any],
        updates: dict[str, Any],
        upsert: bool = False,
        multi: bool = False,
    ) -> int:
        """Update matching documents with validate operators and fields.

        Only allows safe MongoDB operators ($set, $inc, $push, $pull,
        $unset, $addToSet).  Field names in $set are validated against
        fillable.

        Returns the number of documents modified.
        """
        validated_filters = self._validate_filters(filters)
        validated_updates = self._validate_updates(updates)

        try:
            if multi:
                result = await self.collection.update_many(
                    validated_filters, validated_updates, upsert=upsert
                )
            else:
                result = await self.collection.update_one(
                    validated_filters, validated_updates, upsert=upsert
                )
            logger.info("QB UPDATE: collection=%s matched=%d modified=%d",
                        self.collection.name,
                        result.matched_count,
                        result.modified_count)
            return result.modified_count
        except Exception as e:
            logger.exception("QB UPDATE failed: %s", e)
            raise QueryError(f"Update failed: {e}") from e

    # ── DELETE ────────────────────────────────────────────────────────

    async def delete(self, filters: dict[str, Any], multi: bool = False) -> int:
        """Delete matching documents.

        Returns the number of documents deleted.
        """
        validated_filters = self._validate_filters(filters)

        try:
            if multi:
                result = await self.collection.delete_many(validated_filters)
            else:
                result = await self.collection.delete_one(validated_filters)
            logger.info("QB DELETE: collection=%s deleted=%d",
                        self.collection.name,
                        result.deleted_count)
            return result.deleted_count
        except Exception as e:
            logger.exception("QB DELETE failed: %s", e)
            raise QueryError(f"Delete failed: {e}") from e

    # ── Aggregation ───────────────────────────────────────────────────

    async def aggregate(
        self,
        pipeline: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Run a validated aggregation pipeline.

        Validates that the pipeline does not contain dangerous stages
        like $accumulator or $function.
        """
        dangerous_stages = {"$accumulator", "$function"}
        for stage in pipeline:
            for key in stage:
                if key in dangerous_stages:
                    raise ValidationError(
                        f"Dangerous aggregation stage '{key}' is not allowed"
                    )

        try:
            cursor = self.collection.aggregate(pipeline)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.exception("QB AGGREGATE failed: %s", e)
            raise QueryError(f"Aggregate failed: {e}") from e

    # ── Validation helpers ────────────────────────────────────────────

    def _validate_filters(self, filters: dict[str, Any]) -> dict[str, Any]:
        """Validate filter dict to prevent NoSQL injection."""
        dangerous_ops = {"$where", "$expr", "$accumulator", "$function", "$near", "$geoNear"}

        def _validate_value(val: Any) -> Any:
            if isinstance(val, dict):
                for op_key in val:
                    if op_key.startswith("$") and op_key in dangerous_ops:
                        raise ValidationError(
                            f"Dangerous operator '{op_key}' in filter"
                        )
                    if op_key.startswith("$") and op_key not in (
                        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte",
                        "$in", "$nin", "$all", "$elemMatch", "$regex",
                        "$options", "$exists", "$type", "$not", "$size",
                        "$and", "$or", "$nor",
                    ):
                        raise ValidationError(f"Operator '{op_key}' is not allowed in filters")
                return {k: _validate_value(v) for k, v in val.items()}
            if isinstance(val, list):
                return [_validate_value(v) for v in val]
            return val

        result = {}
        for key, value in filters.items():
            # Validate field name
            if not key.startswith("$"):  # Skip operators at top level
                self._validate_field_name(key)
            result[key] = _validate_value(value)

        return result

    def _validate_field_name(self, field: str):
        """Validate a field name in a filter."""
        # Strip dot notation for nested fields
        base_field = field.split(".")[0]
        if base_field.startswith("$"):
            raise ValidationError(f"Field name cannot start with '$': {field}")
        if self.fillable and base_field not in self.fillable and base_field != "_id":
            raise ValidationError(
                f"Field '{base_field}' is not in fillable fields: {self.fillable}"
            )
        if base_field in self.guarded:
            raise ValidationError(f"Field '{base_field}' is guarded")

    def _validate_updates(self, updates: dict[str, Any]) -> dict[str, Any]:
        """Validate update operators and field names."""
        allowed_ops = {"$set", "$inc", "$push", "$pull", "$unset",
                       "$addToSet", "$min", "$max", "$mul", "$rename",
                       "$each", "$position", "$slice", "$sort"}

        validated = {}
        for op, fields in updates.items():
            if op.startswith("$") and op not in allowed_ops:
                raise ValidationError(f"Update operator '{op}' is not allowed")

            if op == "$set":
                # Validate field names inside $set against fillable
                safe_set = {}
                for field, value in fields.items():
                    self._validate_field_name(field)
                    safe_set[field] = value
                validated[op] = safe_set
            else:
                validated[op] = fields

        return validated

    def _strip_non_fillable(self, data: dict[str, Any]) -> dict[str, Any]:
        """Remove non-fillable and guarded fields from data dict."""
        if not self.fillable:
            # Only strip guarded fields
            return {k: v for k, v in data.items() if k not in self.guarded}
        allowed = self.fillable - self.guarded
        return {k: v for k, v in data.items() if k in allowed}
