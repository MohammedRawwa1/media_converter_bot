# models.py
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

try:
    from pymongo import IndexModel
except ImportError:
    IndexModel = None
import logging

logger = logging.getLogger(__name__)


class MediaConversionModel:
    """MongoDB model for tracking media conversions."""

    def __init__(self, mongo_client, db_name="media_conversion_bot"):
        self.db = mongo_client[db_name]
        self.conversions = self.db["conversions"]
        self.users = self.db["users"]
        self.stats = self.db["stats"]

        # Create indexes
        self._create_indexes()

    def _create_indexes(self):
        """Create necessary indexes for collections."""
        # Conversions collection indexes
        self.conversions.create_index([("user_id", 1), ("timestamp", -1)])
        self.conversions.create_index([("action", 1), ("success", 1)])
        self.conversions.create_index(
            [("timestamp", -1)], expireAfterSeconds=30 * 24 * 60 * 60
        )  # Auto-delete after 30 days

        # Users collection indexes
        self.users.create_index("user_id", unique=True)

        # Stats collection indexes
        self.stats.create_index("date", unique=True)

    async def log_conversion(self, conversion_data: Dict[str, Any]) -> str:
        """Log a media conversion event."""
        try:
            conversion_data["timestamp"] = datetime.utcnow()
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

            await self.users.update_one({"user_id": user_id}, update_data, upsert=True)
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

            await self.stats.update_one({"date": today}, update_data, upsert=True)
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")

    async def get_user_stats(self, user_id: int) -> Optional[Dict]:
        """Get user statistics."""
        try:
            user_data = await self.users.find_one(
                {"user_id": user_id}, {"_id": 0, "stats": 1, "first_seen": 1, "last_activity": 1}
            )
            return user_data
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return None

    async def get_recent_conversions(self, user_id: int, limit: int = 10) -> list:
        """Get recent conversions for a user."""
        try:
            cursor = (
                self.conversions.find(
                    {"user_id": user_id},
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

            stats_data = await self.stats.find_one({"date": date}, {"_id": 0})

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
            pipeline = [
                {"$group": {"_id": "$action", "count": {"$sum": 1}, "total_size": {"$sum": "$input_size"}}},
                {"$sort": {"count": -1}},
                {"$limit": limit},
                {"$project": {"action": "$_id", "count": 1, "total_size": 1, "_id": 0}},
            ]

            cursor = self.conversions.aggregate(pipeline)
            results = await cursor.to_list(length=limit)
            return results
        except Exception as e:
            logger.error(f"Error getting top actions: {e}")
            return []

    async def get_conversion_success_rate(self) -> Dict:
        """Get conversion success rate statistics."""
        try:
            pipeline = [
                {"$group": {"_id": "$success", "count": {"$sum": 1}}},
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": "$count"},
                        "successful": {"$sum": {"$cond": [{"$eq": ["$_id", True]}, "$count", 0]}},
                        "failed": {"$sum": {"$cond": [{"$eq": ["$_id", False]}, "$count", 0]}},
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
            ]

            cursor = self.conversions.aggregate(pipeline)
            results = await cursor.to_list(length=1)
            return results[0] if results else {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}
        except Exception as e:
            logger.error(f"Error getting success rate: {e}")
            return {"total": 0, "successful": 0, "failed": 0, "success_rate": 0}

    async def get_storage_usage(self) -> Dict:
        """Get total storage usage statistics."""
        try:
            pipeline = [
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
            ]

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
            result = await self.conversions.delete_many({"timestamp": {"$lt": cutoff_date}})

            logger.info(f"Cleaned up {result.deleted_count} old conversions")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0
