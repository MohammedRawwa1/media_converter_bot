"""Tests for the Telegram file_id caching mechanism.

The file_id cache is a critical optimization that:
1. Reduces IDrive/object storage egress by reusing Telegram's cached file_ids
2. Speeds up media delivery (Telegram serves from their cache)
3. Is free (Telegram caches files on their servers)

This test suite verifies:
- Cache HIT/MISS behavior
- Content identity computation (file_unique_id vs file hash)
- Storing and retrieving file_ids
- send_cached_media convenience function
- Graceful fallback when Redis is unavailable
"""

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from utils import file_id_cache


class TestFileIdCacheContentIdentity:
    """Test content identity computation for cache keys."""

    def test_compute_content_hash_with_file_unique_id(self):
        """file_unique_id should be used as the content identity when provided."""
        identity = file_id_cache._compute_content_hash(file_unique_id="ABC123")
        assert identity == "uid:ABC123"

    def test_compute_content_hash_with_file_path(self):
        """When no file_unique_id, hash the file content."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
            f.write(b"test video content" * 100)
            temp_path = f.name

        try:
            identity = file_id_cache._compute_content_hash(file_path=temp_path)
            assert identity is not None
            assert identity.startswith("hash:")
            # Verify the hash is deterministic
            identity2 = file_id_cache._compute_content_hash(file_path=temp_path)
            assert identity == identity2
        finally:
            os.unlink(temp_path)

    def test_compute_content_hash_none_when_nothing_available(self):
        """Return None when neither file_unique_id nor file_path is provided."""
        identity = file_id_cache._compute_content_hash()
        assert identity is None

    def test_compute_content_hash_prefers_file_unique_id(self):
        """file_unique_id should take precedence over file_path."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
            f.write(b"test video content")
            temp_path = f.name

        try:
            identity = file_id_cache._compute_content_hash(file_unique_id="UID123", file_path=temp_path)
            assert identity == "uid:UID123"
        finally:
            os.unlink(temp_path)

    def test_compute_content_hash_returns_none_for_missing_file(self):
        """Return None for non-existent file path."""
        identity = file_id_cache._compute_content_hash(file_path="/nonexistent/path.mp4")
        assert identity is None


class TestFileIdCacheKeys:
    """Test Redis key generation."""

    def test_cache_prefix(self):
        """Test cache prefix generation for different media types."""
        assert file_id_cache._cache_prefix("photo") == "file_id:photo:"
        assert file_id_cache._cache_prefix("video") == "file_id:video:"
        assert file_id_cache._cache_prefix("audio") == "file_id:audio:"

    def test_file_id_key(self):
        """Test full key generation."""
        key = file_id_cache._file_id_key("video", "uid:ABC123")
        assert key == "file_id:video:uid:ABC123"


class TestFileIdCacheStorage:
    """Test storing and retrieving file_ids."""

    @pytest.mark.asyncio
    async def test_get_file_id_unsupported_type(self):
        """Return None for unsupported media types."""
        result = await file_id_cache.get_file_id("unsupported_type")
        assert result is None

    @pytest.mark.asyncio
    async def test_store_file_id_unsupported_type(self):
        """Return False for unsupported media types."""
        result = await file_id_cache.store_file_id("unsupported_type", "file_id_123")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_file_id_no_content_identity(self):
        """Return None when no content identity can be computed."""
        result = await file_id_cache.get_file_id("photo")
        assert result is None

    @pytest.mark.asyncio
    async def test_store_file_id_no_content_identity(self):
        """Return False when no content identity can be computed."""
        result = await file_id_cache.store_file_id("photo", "file_id_123")
        assert result is False

    @pytest.mark.asyncio
    async def test_store_and_retrieve_file_id(self):
        """Full cycle: store a file_id and retrieve it."""
        file_unique_id = "test_unique_id_123"
        file_id = "telegram_file_id_456"
        content_identity = file_id_cache._compute_content_hash(file_unique_id=file_unique_id)

        assert content_identity is not None

        # Mock the cache client
        mock_cache = AsyncMock()
        mock_cache.get.return_value = None  # MISS
        mock_cache.set.return_value = True

        with patch("utils.file_id_cache._get_cache", return_value=mock_cache):
            # Store
            stored = await file_id_cache.store_file_id(
                "video",
                file_id,
                file_unique_id=file_unique_id,
            )
            assert stored is True
            mock_cache.set.assert_called_once()

            # Retrieve
            mock_cache.get.reset_mock()
            mock_cache.get.return_value = file_id
            retrieved = await file_id_cache.get_file_id(
                "video",
                file_unique_id=file_unique_id,
            )
            assert retrieved == file_id

    @pytest.mark.asyncio
    async def test_invalidate_file_id(self):
        """Test invalidating a cached file_id."""
        file_unique_id = "test_unique_id_789"

        mock_cache = AsyncMock()
        mock_cache.delete.return_value = True

        with patch("utils.file_id_cache._get_cache", return_value=mock_cache):
            invalidated = await file_id_cache.invalidate_file_id(
                "audio",
                file_unique_id=file_unique_id,
            )
            assert invalidated is True
            mock_cache.delete.assert_called_once()


class TestFileIdCacheSendCachedMedia:
    """Test the send_cached_media convenience function."""

    @pytest.mark.asyncio
    async def test_send_cached_media_unsupported_type(self):
        """Return error for unsupported media types."""
        mock_bot = MagicMock()
        result = await file_id_cache.send_cached_media(
            mock_bot,
            chat_id=123,
            media_type="unsupported",
            send_method="send_photo",
        )
        assert result["success"] is False
        assert "Unsupported media type" in result["error"]

    @pytest.mark.asyncio
    async def test_send_cached_media_unknown_send_method(self):
        """Return error for unknown send method."""
        mock_bot = MagicMock()
        # Ensure send_photo doesn't exist as a method
        mock_bot.send_photo = None
        result = await file_id_cache.send_cached_media(
            mock_bot,
            chat_id=123,
            media_type="photo",
            send_method="send_photo",
        )
        assert result["success"] is False
        assert "Unknown send method" in result["error"]

    @pytest.mark.asyncio
    async def test_send_cached_media_cache_hit(self):
        """Use cached file_id when available."""
        mock_bot = MagicMock()
        mock_message = MagicMock()
        mock_video = MagicMock()
        mock_video.file_id = "cached_file_id_123"
        mock_message.video = mock_video
        mock_bot.send_video = AsyncMock(return_value=mock_message)

        file_unique_id = "source_unique_id"
        cached_file_id = "cached_telegram_file_id"

        with patch("utils.file_id_cache.get_file_id", return_value=cached_file_id):
            result = await file_id_cache.send_cached_media(
                mock_bot,
                chat_id=456,
                media_type="video",
                send_method="send_video",
                file_unique_id=file_unique_id,
                caption="Test caption",
            )

        assert result["success"] is True
        assert result["used_cached"] is True
        assert result["file_id"] == "cached_file_id_123"
        mock_bot.send_video.assert_called_once()
        # Verify file_id was passed, not a file object
        call_kwargs = mock_bot.send_video.call_args.kwargs
        assert call_kwargs.get("video") == cached_file_id

    @pytest.mark.asyncio
    async def test_send_cached_media_cache_miss_uploads_fresh(self):
        """Upload fresh file when cache misses."""
        mock_bot = MagicMock()
        mock_message = MagicMock()
        mock_video = MagicMock()
        mock_video.file_id = "new_file_id_from_telegram"
        mock_message.video = mock_video
        mock_bot.send_video = AsyncMock(return_value=mock_message)

        file_unique_id = "source_unique_id"
        file_content = b"video file content here"
        temp_path = None

        # Create a temp file
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
            f.write(file_content)
            temp_path = f.name

        try:
            with (
                patch("utils.file_id_cache.get_file_id", return_value=None),
                patch("utils.file_id_cache.store_file_id", return_value=True),
            ):
                result = await file_id_cache.send_cached_media(
                    mock_bot,
                    chat_id=456,
                    media_type="video",
                    send_method="send_video",
                    file_unique_id=file_unique_id,
                    file_path=temp_path,
                    caption="Test caption",
                )

            assert result["success"] is True
            assert result["used_cached"] is False
            assert result["file_id"] == "new_file_id_from_telegram"
            mock_bot.send_video.assert_called_once()

        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_send_cached_media_no_file_path_on_miss(self):
        """Return error when cache misses and no file path is provided."""
        with patch("utils.file_id_cache.get_file_id", return_value=None):
            result = await file_id_cache.send_cached_media(
                MagicMock(),
                chat_id=123,
                media_type="photo",
                send_method="send_photo",
            )

        assert result["success"] is False
        assert "No file available" in result["error"]

    @pytest.mark.asyncio
    async def test_send_cached_media_photo_extracts_file_id_correctly(self):
        """Photo messages have a photo list; extract the last (highest res)."""
        mock_bot = MagicMock()
        mock_message = MagicMock()
        mock_photo_1 = MagicMock()
        mock_photo_1.file_id = "low_res_file_id"
        mock_photo_2 = MagicMock()
        mock_photo_2.file_id = "high_res_file_id"
        mock_message.photo = [mock_photo_1, mock_photo_2]
        mock_bot.send_photo = AsyncMock(return_value=mock_message)

        file_unique_id = "photo_unique_id"
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".jpg") as f:
            f.write(b"photo content")
            temp_path = f.name

        try:
            with patch("utils.file_id_cache.get_file_id", return_value=None):
                result = await file_id_cache.send_cached_media(
                    mock_bot,
                    chat_id=789,
                    media_type="photo",
                    send_method="send_photo",
                    file_unique_id=file_unique_id,
                    file_path=temp_path,
                )

            assert result["success"] is True
            # The extracted file_id should be from the last photo (highest res)
            assert result["file_id"] == "high_res_file_id"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_send_cached_media_different_media_types(self):
        """Test that different media types work with send_cached_media."""
        file_unique_id = "test_unique_id"

        for media_type, send_method, attr_name in [
            ("photo", "send_photo", "photo"),
            ("video", "send_video", "video"),
            ("audio", "send_audio", "audio"),
            ("document", "send_document", "document"),
            ("voice", "send_voice", "voice"),
        ]:
            mock_bot = MagicMock()
            mock_message = MagicMock()
            mock_media = MagicMock()
            mock_media.file_id = f"{media_type}_file_id"
            setattr(mock_message, attr_name, mock_media)
            mock_bot.send_method = AsyncMock(return_value=mock_message)

            # Set the actual method on the mock
            actual_method = getattr(mock_bot, send_method)
            actual_method = AsyncMock(return_value=mock_message)
            setattr(mock_bot, send_method, actual_method)

            with patch("utils.file_id_cache.get_file_id", return_value=None):
                result = await file_id_cache.send_cached_media(
                    mock_bot,
                    chat_id=123,
                    media_type=media_type,
                    send_method=send_method,
                    file_unique_id=file_unique_id,
                )

            # Should fail gracefully due to no file path
            assert result["success"] is False


class TestFileIdCacheIntegration:
    """Integration tests for the file_id cache with realistic scenarios."""

    @pytest.mark.asyncio
    async def test_welcome_video_reuse_scenario(self):
        """Simulate sending a welcome video to multiple users.

        This is the primary use case: a static welcome video sent to many users
        should reuse the same file_id to avoid repeated egress.
        """
        # Create a temporary file that exists
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
            f.write(b"fake video content")
            welcome_video_path = f.name

        try:
            file_unique_id = "welcome_video_unique"

            user_chats = [1001, 1002, 1003, 1004, 1005]

            call_log = []
            uploaded_file_ids = []

            async def mock_send_video(chat_id, **kwargs):
                video_arg = kwargs.get("video")
                # A cached file_id is a string that starts with "cached_"
                is_cached = isinstance(video_arg, str) and video_arg.startswith("cached_")
                call_log.append({"chat_id": chat_id, "used_file_id": video_arg, "used_cached": is_cached})
                mock_msg = MagicMock()
                mock_video = MagicMock()
                if is_cached:
                    # Return the cached file_id as-is
                    mock_video.file_id = video_arg
                else:
                    # Fresh upload - generate a new file_id starting with "fresh_"
                    new_file_id = f"fresh_{chat_id}"
                    uploaded_file_ids.append(new_file_id)
                    mock_video.file_id = new_file_id
                mock_msg.video = mock_video
                return mock_msg

            mock_bot = MagicMock()
            mock_bot.send_video = mock_send_video

            # Track what's happening at each step
            cache_miss = True
            stored_file_id = None

            for chat_id in user_chats:
                # Mock the cache for each call
                with (
                    patch("utils.file_id_cache._get_cache") as mock_get_cache,
                    patch("utils.file_id_cache.get_file_id") as mock_get_file_id,
                    patch("utils.file_id_cache.store_file_id") as mock_store_file_id,
                ):

                    async def mock_get_file_id_fn(
                        media_type, file_unique_id=None, file_path=None, content_identity=None
                    ):
                        nonlocal cache_miss
                        if cache_miss:
                            cache_miss = False
                            return None  # Cache miss
                        return stored_file_id  # Cache hit - stored_file_id is now "cached_..."

                    async def mock_store_file_id_fn(
                        media_type, file_id, file_unique_id=None, file_path=None, content_identity=None, ttl=None
                    ):
                        nonlocal stored_file_id
                        # Store with a "cached_" prefix to distinguish from fresh uploads
                        stored_file_id = f"cached_{file_id}"
                        return True

                    mock_get_file_id.side_effect = mock_get_file_id_fn
                    mock_store_file_id.side_effect = mock_store_file_id_fn
                    mock_cache = AsyncMock()
                    mock_get_cache.return_value = mock_cache

                    await file_id_cache.send_cached_media(
                        mock_bot,
                        chat_id=chat_id,
                        media_type="video",
                        send_method="send_video",
                        file_unique_id=file_unique_id,
                        file_path=welcome_video_path,
                        caption="Welcome to the bot!",
                    )

            # First user: cache miss (uploaded fresh)
            assert call_log[0]["used_cached"] is False, (
                f"Expected cache miss on first call, got used_cached={call_log[0]['used_cached']}"
            )

            # Subsequent users: cache hit (used cached file_id)
            for i in range(1, len(call_log)):
                assert call_log[i]["used_cached"] is True, (
                    f"Expected cache hit on call {i}, got used_cached={call_log[i]['used_cached']}"
                )
                assert call_log[i]["used_file_id"].startswith("cached_"), (
                    f"Expected cached file_id on call {i}, got {call_log[i]['used_file_id']}"
                )

            # Verify only one fresh upload happened (the first call)
            fresh_uploads = [c for c in call_log if c["used_cached"] is False]
            assert len(fresh_uploads) == 1, f"Expected 1 fresh upload, got {len(fresh_uploads)}"

            # Verify 4 cache hits (for the remaining 4 users)
            cache_hits = [c for c in call_log if c["used_cached"] is True]
            assert len(cache_hits) == 4, f"Expected 4 cache hits, got {len(cache_hits)}"
        finally:
            os.unlink(welcome_video_path)

    @pytest.mark.asyncio
    async def test_different_media_same_content_different_file_ids(self):
        """Different media content should have different cache keys."""
        content1 = b"video content 1"
        content2 = b"video content 2"

        temp_paths = []
        try:
            for content in (content1, content2):
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
                    f.write(content)
                    temp_paths.append(f.name)

            identities = [file_id_cache._compute_content_hash(file_path=p) for p in temp_paths]

            # Each file should have a unique identity
            assert len(set(identities)) == 2
        finally:
            for p in temp_paths:
                if os.path.exists(p):
                    os.unlink(p)

    @pytest.mark.asyncio
    async def test_cache_ttl_respected(self):
        """TTL parameter should be passed to the cache client."""
        mock_cache = AsyncMock()
        mock_cache.set.return_value = True

        with patch("utils.file_id_cache._get_cache", return_value=mock_cache):
            await file_id_cache.store_file_id(
                "video",
                "file_id_123",
                content_identity="test_identity",
                ttl=3600,
            )

        # Verify TTL was passed
        mock_cache.set.assert_called_once()
        call_kwargs = mock_cache.set.call_args.kwargs
        assert call_kwargs.get("ttl") == 3600

    @pytest.mark.asyncio
    async def test_default_ttl_used_when_not_specified(self):
        """Default TTL should be used when not specified."""
        mock_cache = AsyncMock()
        mock_cache.set.return_value = True

        with patch("utils.file_id_cache._get_cache", return_value=mock_cache):
            await file_id_cache.store_file_id(
                "video",
                "file_id_123",
                content_identity="test_identity",
            )

        mock_cache.set.assert_called_once()
        call_kwargs = mock_cache.set.call_args.kwargs
        assert call_kwargs.get("ttl") == file_id_cache.FILE_ID_CACHE_TTL_SECONDS


class TestFileIdCacheGracefulDegradation:
    """Test that the cache degrades gracefully when Redis is unavailable."""

    @pytest.mark.asyncio
    async def test_get_file_id_returns_none_when_cache_unavailable(self):
        """get_file_id should return None when Redis is unavailable."""
        with patch("utils.file_id_cache._get_cache", return_value=None):
            result = await file_id_cache.get_file_id(
                "video",
                content_identity="test_identity",
            )
            assert result is None

    @pytest.mark.asyncio
    async def test_store_file_id_returns_false_when_cache_unavailable(self):
        """store_file_id should return False when Redis is unavailable."""
        with patch("utils.file_id_cache._get_cache", return_value=None):
            result = await file_id_cache.store_file_id(
                "video",
                "file_id_123",
                content_identity="test_identity",
            )
            assert result is False

    @pytest.mark.asyncio
    async def test_send_cached_media_falls_back_to_upload_when_cache_unavailable(self):
        """send_cached_media should upload fresh when cache is unavailable."""
        mock_bot = MagicMock()
        mock_message = MagicMock()
        mock_video = MagicMock()
        mock_video.file_id = "uploaded_file_id"
        mock_message.video = mock_video
        mock_bot.send_video = AsyncMock(return_value=mock_message)

        file_path = None
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".mp4") as f:
            f.write(b"video")
            file_path = f.name

        try:
            with patch("utils.file_id_cache._get_cache", return_value=None):
                result = await file_id_cache.send_cached_media(
                    mock_bot,
                    chat_id=123,
                    media_type="video",
                    send_method="send_video",
                    content_identity="test_identity",
                    file_path=file_path,
                )

            # Should still succeed by uploading fresh
            assert result["success"] is True
            assert result["used_cached"] is False
        finally:
            if os.path.exists(file_path):
                os.unlink(file_path)


class TestFileIdCacheConstants:
    """Test module constants and configuration."""

    def test_supported_media_types(self):
        """Verify all expected media types are supported."""
        assert "photo" in file_id_cache.SUPPORTED_MEDIA_TYPES
        assert "video" in file_id_cache.SUPPORTED_MEDIA_TYPES
        assert "audio" in file_id_cache.SUPPORTED_MEDIA_TYPES
        assert "document" in file_id_cache.SUPPORTED_MEDIA_TYPES
        assert "voice" in file_id_cache.SUPPORTED_MEDIA_TYPES
        assert "sticker" in file_id_cache.SUPPORTED_MEDIA_TYPES

    def test_default_ttl_is_reasonable(self):
        """Default TTL should be a reasonable value (not too short, not too long)."""
        assert 3600 <= file_id_cache.FILE_ID_CACHE_TTL_SECONDS <= 604800  # 1 hour to 1 week
