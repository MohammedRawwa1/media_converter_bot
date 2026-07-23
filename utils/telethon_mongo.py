"""Telethon -> Mongo bridge helper

Provides a small, optional async helper to persist Telethon ingestion metadata
into the existing Mongo model used by the application. Enabled via
`TELETHON_MONGO_BRIDGE=1`.

This module is best-effort and will not raise if Mongo or dependencies
are missing.
"""
import logging
import os

logger = logging.getLogger(__name__)


async def save_telethon_forward(metadata: dict) -> str | None:
    """Save Telethon ingestion metadata to MongoDB using MediaConversionModel.

    Returns inserted id string on success or None on failure.
    This function is best-effort and respects the environment flag
    `TELETHON_MONGO_BRIDGE`.
    """
    if os.environ.get("TELETHON_MONGO_BRIDGE", "").lower() not in ("1", "true", "yes"):
        return None

    # Resolve Mongo URI
    mongo_uri = (
        os.environ.get("MONGO_URI")
        or os.environ.get("MONGODB_URL")
        or os.environ.get("MONGODB_URI")
        or os.environ.get("MONGO_URL")
    )
    if not mongo_uri:
        logger.info("TELETHON_MONGO_BRIDGE enabled but no Mongo URI configured")
        return None

    try:
        from motor.motor_asyncio import AsyncIOMotorClient

        from models import MediaConversionModel
    except Exception as e:
        logger.debug("Mongo dependencies not available for Telethon bridge: %s", e)
        return None

    try:
        client = AsyncIOMotorClient(mongo_uri)
        col_prefix = os.environ.get("MONGODB_COLLECTION_PREFIX")
        if col_prefix:
            model = MediaConversionModel(
                client,
                db_name=os.environ.get("MONGODB_NAME", "media_conversion_bot"),
                collection_prefix=col_prefix,
            )
        else:
            model = MediaConversionModel(client, db_name=os.environ.get("MONGODB_NAME", "media_conversion_bot"))

        doc = dict(metadata)
        doc.setdefault("action", "telethon_ingest")
        doc.setdefault("success", True)
        if "input_size" not in doc and "size" in doc:
            doc["input_size"] = doc.get("size")

        inserted = await model.log_conversion(doc)
        logger.info("Telethon->Mongo saved: %s", inserted)
        return inserted
    except Exception as e:
        logger.exception("Failed to save telethon forward to Mongo: %s", e)
        return None
