import os
import logging

logger = logging.getLogger(__name__)


async def download_file_via_userbot(file_id: str, dest_path: str) -> str:
    """Attempt to download a Telegram file using a user account (userbot).

    Environment variables supported:
      - USERBOT_API_ID (required)
      - USERBOT_API_HASH (required)
      - USERBOT_STRING_SESSION (optional)  # preferred: string session
      - USERBOT_SESSION (optional)         # fallback: session filename

    Returns the path to the downloaded file on success.
    Raises a RuntimeError with a helpful message on failure.
    """
    try:
        # Import pyrogram lazily; provide actionable error if missing
        from pyrogram import Client
    except Exception as e:
        raise RuntimeError(
            "pyrogram is required for userbot downloads. Install it: `pip install pyrogram tgcrypto`"
        ) from e

    api_id = os.environ.get("USERBOT_API_ID") or os.environ.get("API_ID")
    api_hash = os.environ.get("USERBOT_API_HASH") or os.environ.get("API_HASH")
    string_session = os.environ.get("USERBOT_STRING_SESSION") or os.environ.get("USERBOT_SESSION_STRING")
    session_name = os.environ.get("USERBOT_SESSION")

    if not api_id or not api_hash:
        raise RuntimeError(
            "USERBOT_API_ID and USERBOT_API_HASH environment variables are required for userbot downloads."
        )

    # Prefer a provided string session if present (non-interactive).
    client = None
    used_string = False
    try:
        if string_session:
            # Try to use StringSession if available
            try:
                from pyrogram.session import StringSession

                client = Client(StringSession(string_session), api_id=int(api_id), api_hash=api_hash)
                used_string = True
            except Exception:
                # Fall back to passing the string as session name; some pyrogram versions
                # accept it directly when creating the client.
                client = Client(string_session, api_id=int(api_id), api_hash=api_hash)
                used_string = True
        else:
            # Use a session filename (will prompt for login if no session exists)
            if not session_name:
                raise RuntimeError(
                    "No USERBOT_SESSION or USERBOT_STRING_SESSION provided. Provide a string session or session filename."
                )
            client = Client(session_name, api_id=int(api_id), api_hash=api_hash)

        await client.start()
        try:
            # pyrogram's download_media accepts file identifiers and message/file objects.
            saved = await client.download_media(file_id, file_name=dest_path)
            if not saved:
                raise RuntimeError("pyrogram did not return a saved path after download")
            return saved
        finally:
            try:
                await client.stop()
            except Exception:
                pass
    except Exception as e:
        # Ensure client cleaned up on error
        try:
            if client is not None:
                await client.stop()
        except Exception:
            pass
        logger.exception("Userbot download failed")
        raise RuntimeError(f"Userbot download failed: {e}")
