# models.py
"""
Secure MongoDB models with Laravel-style fillable/guarded protection,
parameterized queries (like prepared statements), and schema validation.

Patterns applied:
  - FillableModel mixin: prevents mass-assignment vulnerabilities
  - QueryBuilder: parameterized queries with NoSQL injection prevention
  - SchemaValidator: typed field validation and whitelisting
  - PreparedQuery: Go/Java-style prepared statement patterns
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

# Avoid importing pymongo at module import time; handle missing dependency
# at runtime so the module can be imported even if pymongo isn't installed.
IndexModel = None

logger = logging.getLogger(__name__)

try:
    from bson import ObjectId
except Exception:
    ObjectId = None


import contextlib

from utils.data_layer.fillable import FillableModel
from utils.data_layer.query_builder import QueryBuilder


class MediaConversionModel(FillableModel):
    """MongoDB model for tracking media conversions.

    Inherits FillableModel for Laravel-style $fillable/$guarded
    mass-assignment protection. Uses QueryBuilder for parameterized
    queries (like prepared statements) to prevent NoSQL injection.
    """

    # ── Laravel-style $fillable / $guarded fields ──
    # Only fields in `fillable` can be mass-assigned.
    # Fields in `guarded` are never writable via mass assignment.
    # This prevents injection of `is_admin`, `role` etc. via API payloads.
    fillable: set[str] = {
        "user_id", "action", "file_name", "file_type", "file_size",
        "input_format", "output_format", "input_size", "output_size",
        "success", "processing_time", "timestamp", "username",
        "error_message", "parameters", "chat_id", "message_id",
        "source_format", "target_format", "bot_id",
    }
    guarded: set[str] = {"_id", "is_admin", "role", "permissions"}

    def __init__(self, mongo_client, db_name: str = "media_conversion_bot", bot_id: str = None, collection_prefix: str = None):
        """
        mongo_client: Motor/PyMongo client
        db_name: database name to use
        bot_id: optional identifier to tag documents with (separates multiple bots)
        collection_prefix: optional prefix to use for collection names (alternative separation)
        """
        self.db = mongo_client[db_name]
        self.bot_id = bot_id or os.environ.get("BOT_ID") or os.environ.get("BOT_USERNAME") or None
        prefix = f"{collection_prefix}_" if collection_prefix else ""

        # Raw collections for index creation and direct access
        self._conversions_coll = self.db[f"{prefix}conversions"]
        self._users_coll = self.db[f"{prefix}users"]
        self._stats_coll = self.db[f"{prefix}stats"]
        self._sessions_coll = self.db[f"{prefix}sessions"]
        self._schedules_coll = self.db[f"{prefix}schedules"]

        # ── QueryBuilder instances (Go/Java-style prepared statements) ──
        # These enforce parameterized queries with NoSQL injection prevention.
        # Fillable/guarded sets are passed so field names in filters and
        # projections are validated against the model's whitelist.
        self.conversions = QueryBuilder(
            collection=self._conversions_coll,
            fillable_fields=self.fillable | {"timestamp"},
            guarded_fields=self.guarded,
        )
        self.users = QueryBuilder(
            collection=self._users_coll,
            fillable_fields={"user_id", "bot_id", "stats", "username",
                             "first_seen", "last_activity"},
            guarded_fields=self.guarded,
        )
        self.stats = QueryBuilder(
            collection=self._stats_coll,
            fillable_fields={"date", "bot_id", "total_conversions",
                             "actions", "formats", "total_input_size",
                             "total_output_size"},
            guarded_fields=self.guarded,
        )
        self.sessions = QueryBuilder(
            collection=self._sessions_coll,
            fillable_fields={"user_id", "bot_id", "session", "updated_at"},
            guarded_fields=self.guarded,
        )
        self.schedules = QueryBuilder(
            collection=self._schedules_coll,
            fillable_fields={"run_at", "status", "bot_id", "created_at",
                             "finished_at", "_id"},
            guarded_fields=self.guarded,
        )

        # Index creation is performed asynchronously via `ensure_indexes()`
        # to avoid blocking or spawning background threads that raise
        # exceptions during synchronous init when Mongo is unreachable.

    async def ensure_indexes(self):
        """Asynchronously create necessary indexes for collections.

        This should be scheduled from an async context so failures are
        logged instead of occurring in background threads that produce
        uncaught exceptions when the DB is unreachable.
        """
        try:
            # Index by bot_id + user_id + timestamp for efficient per-bot queries
            if self.bot_id is not None:
                with contextlib.suppress(Exception):
                    await self._conversions_coll.create_index([("bot_id", 1), ("user_id", 1), ("timestamp", -1)])

            with contextlib.suppress(Exception):
                await self._conversions_coll.create_index([("user_id", 1), ("timestamp", -1)])

            with contextlib.suppress(Exception):
                await self._conversions_coll.create_index([("action", 1), ("success", 1)])

            with contextlib.suppress(Exception):
                await self._conversions_coll.create_index([("timestamp", -1)], expireAfterSeconds=30 * 24 * 60 * 60)

            # Users collection indexes
            with contextlib.suppress(Exception):
                await self._users_coll.create_index("user_id", unique=True)

            # Stats collection indexes
            with contextlib.suppress(Exception):
                await self._stats_coll.create_index("date", unique=True)

            # Sessions: index by user_id for fast lookup
            with contextlib.suppress(Exception):
                await self._sessions_coll.create_index("user_id", unique=True)

            # Schedules: index by run_at and status for efficient queries
            with contextlib.suppress(Exception):
                await self._schedules_coll.create_index([("run_at", 1), ("status", 1)])

        except Exception as e:
            logger.warning("Failed to ensure indexes: %s", e)

    async def log_conversion(self, conversion_data: dict[str, Any]) -> str:
        """Log a media conversion event with fillable protection."""
        try:
            # Apply $fillable protection: strip non-fillable and guarded fields
            safe_data = self.filter_fillable(conversion_data)
            safe_data["timestamp"] = datetime.utcnow()
            if self.bot_id is not None:
                safe_data["bot_id"] = self.bot_id

            # Parameterized insert via QueryBuilder (validates fillable fields)
            result_id = await self.conversions.insert(safe_data)

            # Update user stats
            await self._update_user_stats(conversion_data)

            # Update daily stats
            await self._update_daily_stats(conversion_data)

            logger.info(f"Logged conversion: {result_id}")
            return result_id
        except Exception as e:
            logger.error(f"Error logging conversion: {e}")
            return None

    async def _update_user_stats(self, conversion_data: dict[str, Any]):
        """Update user statistics (parameterized via QueryBuilder)."""
        try:
            user_id = conversion_data.get("user_id")
            action = conversion_data.get("action")

            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            update_data = {
                "$inc": {
                    "stats.total_conversions": 1,
                    f"stats.{action}": 1,
                    "stats.total_input_size": conversion_data.get("input_size", 0),
                    "stats.total_output_size": conversion_data.get("output_size", 0),
                },
                "$set": {"last_activity": datetime.utcnow(), "username": conversion_data.get("username")},
                "$setOnInsert": {"user_id": user_id, "first_seen": datetime.utcnow()},
            }
            if self.bot_id is not None:
                update_data.setdefault("$setOnInsert", {})["bot_id"] = self.bot_id

            # Parameterized update via QueryBuilder (validates operators + fields)
            await self.users.update(query, update_data, upsert=True)
        except Exception as e:
            logger.error(f"Error updating user stats: {e}")

    async def _update_daily_stats(self, conversion_data: dict[str, Any]):
        """Update daily statistics (parameterized via QueryBuilder)."""
        try:
            today = datetime.utcnow().date().isoformat()
            action = conversion_data.get("action")

            update_data = {
                "$inc": {
                    "total_conversions": 1,
                    f"actions.{action}": 1,
                    "total_input_size": conversion_data.get("input_size", 0),
                    "total_output_size": conversion_data.get("output_size", 0),
                    f"formats.{conversion_data.get('input_format')}.input": 1,
                    f"formats.{conversion_data.get('output_format')}.output": 1,
                }
            }
            query = {"date": today}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
                update_data.setdefault("$setOnInsert", {})["bot_id"] = self.bot_id

            await self.stats.update(query, update_data, upsert=True)
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")

    async def get_user_stats(self, user_id: int) -> dict | None:
        """Get user statistics."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            user_data = await self.users.select(
                filters=query,
                projection={"_id": 0, "stats": 1, "first_seen": 1, "last_activity": 1},
            ).first()
            return user_data
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return None

    async def get_recent_conversions(self, user_id: int, limit: int = 10) -> list:
        """Get recent conversions for a user."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            conversions = await self.conversions.select(
                filters=query,
                projection={
                    "_id": 0,
                    "action": 1,
                    "input_format": 1,
                    "output_format": 1,
                    "input_size": 1,
                    "output_size": 1,
                    "success": 1,
                    "timestamp": 1,
                    "processing_time": 1,
                },
            ).sort(("timestamp", -1)).limit(limit).to_list(length=limit)
            return conversions
        except Exception as e:
            logger.error(f"Error getting recent conversions: {e}")
            return []

    async def get_daily_stats(self, date: str | None = None) -> dict:
        """Get daily statistics."""
        try:
            if not date:
                date = datetime.utcnow().date().isoformat()

            query = {"date": date}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            stats_data = await self.stats.select(filters=query, projection={"_id": 0}).first()

            if not stats_data:
                return {
                    "date": date,
                    "total_conversions": 0,
                    "actions": {},
                    "formats": {},
                    "total_input_size": 0,
                    "total_output_size": 0,
                }

            return stats_data
        except Exception as e:
            logger.error(f"Error getting daily stats: {e}")
            return {}

    async def get_top_actions(self, limit: int = 5) -> list:
        """Get most popular conversion actions."""
        try:
            pipeline = []
            if self.bot_id is not None:
                pipeline.append({"$match": {"bot_id": self.bot_id}})
            pipeline.extend([
                {"$group": {"_id": "$action", "count": {"$sum": 1}, "total_size": {"$sum": "$input_size"}}},
                {"$sort": {"count": -1}},
                {"$limit": limit},
                {"$project": {"action": "$_id", "count": 1, "total_size": 1, "_id": 0}},
            ])

            results = await self.conversions.aggregate(pipeline)
            return results[:limit]
        except Exception as e:
            logger.error(f"Error getting top actions: {e}")
            return []

    # -------- Session helpers --------
    async def save_session(self, user_id: int, session_data: dict[str, Any]) -> bool:
        """Save a minimal session document for quick recovery across restarts.

        Uses individual ``session.<key>`` paths in ``$set`` so multiple callers
        can update distinct keys within the ``session`` sub-document without
        clobbering each other — critical when saves run in parallel (e.g.
        the healthchecker checks both Telethon and Pyrogram concurrently).
        """
        try:
            set_fields = {"updated_at": datetime.utcnow()}
            for key, value in session_data.items():
                set_fields[f"session.{key}"] = value
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
                set_fields["bot_id"] = self.bot_id
            set_fields["user_id"] = user_id
            await self.sessions.update(query, {"$set": set_fields}, upsert=True)
            return True
        except Exception as e:
            # Allow quieter logging during Mongo outages. Set QUIET_MONGO_SESSION_ERRORS=1
            # in the environment to avoid noisy ERROR-level logs while connectivity
            # is being repaired. Debug mode still records the traceback.
            try:
                quiet = os.environ.get("QUIET_MONGO_SESSION_ERRORS", "").lower() in ("1", "true", "yes")
            except Exception:
                quiet = False
            if quiet:
                logger.debug("Error saving session for %s: %s", user_id, e, exc_info=True)
            else:
                logger.error("Error saving session for %s: %s", user_id, e, exc_info=True)
            return False

    async def load_session(self, user_id: int) -> dict[str, Any] | None:
        """Load a previously saved session for a user_id."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            doc = await self.sessions.select(filters=query, projection={"_id": 0, "session": 1}).first()
            if doc:
                return doc.get("session")
            return None
        except Exception as e:
            logger.error("Error loading session for %s: %s", user_id, e)
            return None

    async def delete_session(self, user_id: int) -> bool:
        """Delete a persisted session for a user."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            await self.sessions.delete(query)
            return True
        except Exception as e:
            logger.error("Error deleting session for %s: %s", user_id, e)
            return False

    # -------- Scheduled activities --------
    async def schedule_activity(self, activity: dict[str, Any]) -> str | None:
        """Insert a scheduled activity document with fillable protection.

        Required fields: run_at (datetime). Returns inserted id string on success.
        """
        try:
            # Apply $fillable protection
            safe_data = self.filter_fillable(activity)
            safe_data.setdefault("created_at", datetime.utcnow())
            safe_data.setdefault("status", "pending")
            if self.bot_id is not None:
                safe_data["bot_id"] = self.bot_id
            res_id = await self.schedules.insert(safe_data)
            return res_id
        except Exception as e:
            logger.error("Error scheduling activity: %s", e)
            return None

    async def get_due_activities(self, upto: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Return activities with run_at <= upto and status pending."""
        try:
            if upto is None:
                upto = datetime.utcnow()
            query = {"run_at": {"$lte": upto}, "status": "pending"}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            results = await self.schedules.select(filters=query).sort(("run_at", 1)).limit(limit).to_list(length=limit)
            return results
        except Exception as e:
            logger.error("Error fetching due activities: %s", e)
            return []

    async def mark_activity_done(self, activity_id: str) -> bool:
        """Mark a scheduled activity as done (delete or set status=done)."""
        try:
            query = {"_id": ObjectId(activity_id)} if ObjectId is not None else {"_id": activity_id}
            await self.schedules.update(query, {"$set": {"status": "done", "finished_at": datetime.utcnow()}})
            return True
        except Exception as e:
            logger.error("Error marking activity done %s: %s", activity_id, e)
            return False

    async def get_conversion_success_rate(self) -> dict:
        """Get conversion success rate statistics."""
        try:
            pipeline = []
            if self.bot_id is not None:
                pipeline.append({"$match": {"bot_id": self.bot_id}})
            pipeline.extend([
                {"$group": {"_id": "$success", "count": {"$sum": 1}}},
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": "$count"},
                        "successful": {"$sum": {"$cond": [{"$eq": ["$_id", True]}, "$count", 0]}} ,
                        "failed": {"$sum": {"$cond": [{"$eq": ["$_id", False]}, "$count", 0]}} ,
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "total": 1,
                        "successful": 1,
                        "failed": 1,
                        "success_rate": {"$multiply": [{"$divide": ["$successful", "$total"]}, 100]},
                    }
                },
            ])

            results = await self.conversions.aggregate(pipeline)
            return results[0] if results else {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}
        except Exception as e:
            logger.error(f"Error getting success rate: {e}")
            return {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}

    async def get_storage_usage(self) -> dict:
        """Get total storage usage statistics."""
        try:
            pipeline = []
            if self.bot_id is not None:
                pipeline.append({"$match": {"bot_id": self.bot_id}})
            pipeline.extend([
                {
                    "$group": {
                        "_id": None,
                        "total_input_size": {"$sum": "$input_size"},
                        "total_output_size": {"$sum": "$output_size"},
                        "total_files": {"$sum": 1},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "total_input_size": 1,
                        "total_output_size": 1,
                        "total_files": 1,
                        "compression_ratio": {
                            "$cond": [
                                {"$eq": ["$total_input_size", 0]},
                                0,
                                {
                                    "$multiply": [
                                        {
                                            "$divide": [
                                                {"$subtract": ["$total_input_size", "$total_output_size"]},
                                                "$total_input_size",
                                            ]
                                        },
                                        100,
                                    ]
                                },
                            ]
                        },
                    }
                },
            ])

            results = await self.conversions.aggregate(pipeline)
            return (
                results[0]
                if results
                else {"total_input_size": 0, "total_output_size": 0, "total_files": 0, "compression_ratio": 0}
            )
        except Exception as e:
            logger.error(f"Error getting storage usage: {e}")
            return {"total_input_size": 0, "total_output_size": 0, "total_files": 0, "compression_ratio": 0}

    async def cleanup_old_data(self, days: int = 30) -> int:
        """Clean up data older than specified days."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)

            # Delete old conversions
            query = {"timestamp": {"$lt": cutoff_date}}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            deleted = await self.conversions.delete(query, multi=True)

            logger.info(f"Cleaned up {deleted} old conversions")
            return deleted
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0
