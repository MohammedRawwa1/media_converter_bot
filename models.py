# models.py
from datetime import datetime, timedelta
import os
from typing import Any, Dict, Optional, List
import logging

# Avoid importing pymongo at module import time; handle missing dependency
# at runtime so the module can be imported even if pymongo isn't installed.
IndexModel = None

logger = logging.getLogger(__name__)

try:
    from bson import ObjectId
except Exception:
    ObjectId = None


class MediaConversionModel:
    """MongoDB model for tracking media conversions."""

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
        self.conversions = self.db[f"{prefix}conversions"]
        self.users = self.db[f"{prefix}users"]
        self.stats = self.db[f"{prefix}stats"]
        # Sessions collection for saving ephemeral user sessions and activity state
        self.sessions = self.db[f"{prefix}sessions"]
        # Scheduled activities (run_at: ISO datetime, status: pending|running|done)
        self.schedules = self.db[f"{prefix}schedules"]

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
                try:
                    await self.conversions.create_index([("bot_id", 1), ("user_id", 1), ("timestamp", -1)])
                except Exception:
                    pass

            try:
                await self.conversions.create_index([("user_id", 1), ("timestamp", -1)])
            except Exception:
                pass

            try:
                await self.conversions.create_index([("action", 1), ("success", 1)])
            except Exception:
                pass

            try:
                await self.conversions.create_index([("timestamp", -1)], expireAfterSeconds=30 * 24 * 60 * 60)
            except Exception:
                pass

            # Users collection indexes
            try:
                await self.users.create_index("user_id", unique=True)
            except Exception:
                pass

            # Stats collection indexes
            try:
                await self.stats.create_index("date", unique=True)
            except Exception:
                pass

            # Sessions: index by user_id for fast lookup
            try:
                await self.sessions.create_index("user_id", unique=True)
            except Exception:
                pass

            # Schedules: index by run_at and status for efficient queries
            try:
                await self.schedules.create_index([("run_at", 1), ("status", 1)])
            except Exception:
                pass

        except Exception as e:
            logger.warning("Failed to ensure indexes: %s", e)

    async def log_conversion(self, conversion_data: Dict[str, Any]) -> str:
        """Log a media conversion event."""
        try:
            conversion_data["timestamp"] = datetime.utcnow()
            # Tag with bot_id when present so multiple bots can share the same DB
            if self.bot_id is not None:
                conversion_data["bot_id"] = self.bot_id

            result = await self.conversions.insert_one(conversion_data)

            # Update user stats
            await self._update_user_stats(conversion_data)

            # Update daily stats
            await self._update_daily_stats(conversion_data)

            logger.info(f"Logged conversion: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error logging conversion: {e}")
            return None

    async def _update_user_stats(self, conversion_data: Dict[str, Any]):
        """Update user statistics."""
        try:
            user_id = conversion_data.get("user_id")
            action = conversion_data.get("action")

            # Update user document
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

            # Include bot_id in the query and on-insert doc when applicable
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
                update_data["$setOnInsert"]["bot_id"] = self.bot_id

            await self.users.update_one(query, update_data, upsert=True)
        except Exception as e:
            logger.error(f"Error updating user stats: {e}")

    async def _update_daily_stats(self, conversion_data: Dict[str, Any]):
        """Update daily statistics."""
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

            await self.stats.update_one(query, update_data, upsert=True)
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")

    async def get_user_stats(self, user_id: int) -> Optional[Dict]:
        """Get user statistics."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            user_data = await self.users.find_one(query, {"_id": 0, "stats": 1, "first_seen": 1, "last_activity": 1})
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

            cursor = (
                self.conversions.find(
                    query,
                    {
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
                )
                .sort("timestamp", -1)
                .limit(limit)
            )

            conversions = await cursor.to_list(length=limit)
            return conversions
        except Exception as e:
            logger.error(f"Error getting recent conversions: {e}")
            return []

    async def get_daily_stats(self, date: Optional[str] = None) -> Dict:
        """Get daily statistics."""
        try:
            if not date:
                date = datetime.utcnow().date().isoformat()

            query = {"date": date}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id

            stats_data = await self.stats.find_one(query, {"_id": 0})

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

            cursor = self.conversions.aggregate(pipeline)
            results = await cursor.to_list(length=limit)
            return results
        except Exception as e:
            logger.error(f"Error getting top actions: {e}")
            return []

    # -------- Session helpers --------
    async def save_session(self, user_id: int, session_data: Dict[str, Any]) -> bool:
        """Save a minimal session document for quick recovery across restarts."""
        try:
            doc = {"user_id": user_id, "session": session_data, "updated_at": datetime.utcnow()}
            if self.bot_id is not None:
                doc["bot_id"] = self.bot_id
            await self.sessions.update_one({"user_id": user_id, **({"bot_id": self.bot_id} if self.bot_id is not None else {})}, {"$set": doc}, upsert=True)
            return True
        except Exception as e:
            logger.error(f"Error saving session for %s: %s", user_id, e)
            return False

    async def load_session(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Load a previously saved session for a user_id."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            doc = await self.sessions.find_one(query, {"_id": 0, "session": 1})
            if doc:
                return doc.get("session")
            return None
        except Exception as e:
            logger.error(f"Error loading session for %s: %s", user_id, e)
            return None

    async def delete_session(self, user_id: int) -> bool:
        """Delete a persisted session for a user."""
        try:
            query = {"user_id": user_id}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            await self.sessions.delete_one(query)
            return True
        except Exception as e:
            logger.error(f"Error deleting session for %s: %s", user_id, e)
            return False

    # -------- Scheduled activities --------
    async def schedule_activity(self, activity: Dict[str, Any]) -> Optional[str]:
        """Insert a scheduled activity document. Required fields: run_at (datetime).

        Returns inserted id string on success.
        """
        try:
            activity = dict(activity)
            activity.setdefault("created_at", datetime.utcnow())
            activity.setdefault("status", "pending")
            if self.bot_id is not None:
                activity["bot_id"] = self.bot_id
            res = await self.schedules.insert_one(activity)
            return str(res.inserted_id)
        except Exception as e:
            logger.error(f"Error scheduling activity: %s", e)
            return None

    async def get_due_activities(self, upto: Optional[datetime] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Return activities with run_at <= upto and status pending."""
        try:
            if upto is None:
                upto = datetime.utcnow()
            query = {"run_at": {"$lte": upto}, "status": "pending"}
            if self.bot_id is not None:
                query["bot_id"] = self.bot_id
            cursor = self.schedules.find(query).sort("run_at", 1).limit(limit)
            results = await cursor.to_list(length=limit)
            return results
        except Exception as e:
            logger.error(f"Error fetching due activities: %s", e)
            return []

    async def mark_activity_done(self, activity_id: str) -> bool:
        """Mark a scheduled activity as done (delete or set status=done)."""
        try:
            query = {"_id": ObjectId(activity_id)} if ObjectId is not None else {"_id": activity_id}
            await self.schedules.update_one(query, {"$set": {"status": "done", "finished_at": datetime.utcnow()}})
            return True
        except Exception as e:
            logger.error(f"Error marking activity done %s: %s", activity_id, e)
            return False

    async def get_conversion_success_rate(self) -> Dict:
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

            cursor = self.conversions.aggregate(pipeline)
            results = await cursor.to_list(length=1)
            return results[0] if results else {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}
        except Exception as e:
            logger.error(f"Error getting success rate: {e}")
            return {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}

    async def get_storage_usage(self) -> Dict:
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

            cursor = self.conversions.aggregate(pipeline)
            results = await cursor.to_list(length=1)
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
            result = await self.conversions.delete_many(query)

            logger.info(f"Cleaned up {result.deleted_count} old conversions")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0
