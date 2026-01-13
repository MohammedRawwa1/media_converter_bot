# handlers.py
import asyncio
import os
import tempfile
import json
from typing import Dict, List, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import ContextTypes, ConversationHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest
import logging
from datetime import datetime

# Try to import from local modules
try:
    from media_converter import ExtendedMediaConverter
except ImportError:
    ExtendedMediaConverter = None

try:
    from utils.keyboard_utils import MediaMenuBuilder
except ImportError:
    MediaMenuBuilder = None

try:
    from utils.file_utils import AsyncFileLock
except ImportError:
    AsyncFileLock = None

# Import ACL helper
try:
    from config import is_user_allowed
except Exception:
    def is_user_allowed(_):
        return True

logger = logging.getLogger(__name__)

# Optional ffmpeg-python binding (best-effort)
try:
    import ffmpeg
except Exception:
    ffmpeg = None

# Optional MongoDB model (best-effort import)
try:
    from models import MediaConversionModel
except Exception:
    MediaConversionModel = None

# Conversation states
SELECT_TIME, SELECT_RESOLUTION, SELECT_BITRATE, MERGE_FILES, CUSTOM_INPUT = range(5)

class EnhancedMediaHandler:
    def __init__(self, max_concurrent_conversions: int = 5):
        if ExtendedMediaConverter is None:
            raise ImportError("ExtendedMediaConverter not available")
        self.converter = ExtendedMediaConverter()
        self.user_sessions: Dict[int, Dict] = {}
        self.session_timeouts: Dict[int, asyncio.TimerHandle] = {}
        self.db_model = None  # Optional MongoDB model
        self._session_timeout_seconds = 3600  # 1 hour inactivity timeout
        
        # Concurrency limiter for conversions
        self.conversion_semaphore = asyncio.Semaphore(max_concurrent_conversions)
        self.active_conversions: Dict[int, str] = {}  # user_id -> task_name
    
    async def _cleanup_session(self, user_id: int):
        """Cleanup user session asynchronously."""
        if user_id not in self.user_sessions:
            return
        
        session = self.user_sessions[user_id]
        
        try:
            # Clean temp files
            if 'current_file' in session:
                temp_path = session['current_file'].get('path')
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                        logger.info(f"Cleaned up temp file: {temp_path}")
                    except Exception as e:
                        logger.error(f"Failed to cleanup {temp_path}: {e}")
            
            # Clean merge list files
            if 'merge_list' in session:
                for file_info in session['merge_list']:
                    temp_path = file_info.get('path')
                    if temp_path and os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
        finally:
            # Remove session
            if user_id in self.user_sessions:
                del self.user_sessions[user_id]
            if user_id in self.session_timeouts:
                del self.session_timeouts[user_id]
            
            logger.info(f"Cleaned up session for user {user_id}")
    
    def _schedule_session_cleanup(self, user_id: int):
        """Schedule automatic cleanup for session."""
        # Cancel existing timer
        if user_id in self.session_timeouts:
            self.session_timeouts[user_id].cancel()
        
        # Schedule new cleanup
        try:
            loop = asyncio.get_event_loop()
            handle = loop.call_later(
                self._session_timeout_seconds,
                lambda: asyncio.create_task(self._cleanup_session(user_id))
            )
            self.session_timeouts[user_id] = handle
        except RuntimeError:
            logger.error("Failed to schedule session cleanup - no event loop")
    
    async def _run_with_concurrency_limit(self, user_id: int, task_name: str, coroutine):
        """Run a conversion task with concurrency limiting."""
        async with self.conversion_semaphore:
            self.active_conversions[user_id] = task_name
            try:
                return await coroutine
            finally:
                self.active_conversions.pop(user_id, None)
    
    def get_active_conversions(self) -> Dict[int, str]:
        """Get all active conversions."""
        return self.active_conversions.copy()
    
    async def handle_media_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main entry point for media messages."""
        # Log incoming update for debugging dispatch issues
        try:
            user_id = update.effective_user.id
        except Exception:
            user_id = None

        try:
            logger.info(
                "Incoming message update: user_id=%s update_id=%s msg_id=%s has_video=%s has_document=%s has_audio=%s text=%s",
                user_id,
                getattr(update, 'update_id', None),
                getattr(getattr(update, 'message', None), 'message_id', None),
                bool(getattr(update.message, 'video', None)),
                bool(getattr(update.message, 'document', None)),
                bool(getattr(update.message, 'audio', None)),
                (getattr(update.message, 'text', None) or '')[:200]
            )
        except Exception:
            logger.exception("Failed to log incoming message update")
        # Enforce access control for private bots
        try:
            if not is_user_allowed(user_id):
                await update.message.reply_text(
                    "Access denied. This bot is private.",
                    reply_markup=MediaMenuBuilder.get_main_menu() if MediaMenuBuilder else None
                )
                return
        except Exception:
            # If ACL check fails for any reason, default to deny-safe
            try:
                await update.message.reply_text("Access denied. (ACL check failed)")
            except Exception:
                pass
            return
        
        # Initialize user session
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                'files': {},
                'current_file': None,
                'merge_list': [],
                'processing': False
            }
        
        session = self.user_sessions[user_id]
        
        # Schedule session cleanup (resets timer on each interaction)
        self._schedule_session_cleanup(user_id)
        
        # Check if message has video
        if update.message.video:
            await self.handle_video(update, context, session)
        elif update.message.document:
            await self.handle_document(update, context, session)
        elif update.message.audio:
            await self.handle_audio(update, context, session)
        else:
            await update.message.reply_text("Please send a video, audio, or document file.")
    
    async def handle_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Handle incoming video files."""
        video = update.message.video
        user_id = update.effective_user.id
        
        # Check file size
        max_size = 4 * 1024**3  # 4GB
        if video.file_size > max_size:
            await update.message.reply_text(
                "❌ File too large (max 4GB).\n"
                "For larger files, use the /upload command."
            )
            return
        
        await update.message.reply_text("📥 Downloading video...")
        
        # Download file
        file = await context.bot.get_file(video.file_id)
        ext = '.mp4'  # Telegram videos are usually MP4
        file_path = f"storage/input/{user_id}_{video.file_id}{ext}"
        
        await file.download_to_drive(file_path)
        
        # Store in session
        session['current_file'] = {
            'path': file_path,
            'type': 'video',
            'id': video.file_id,
            'size': video.file_size,
            'name': update.message.caption or f"video_{video.file_id[:8]}.mp4"
        }
        
        # Log to MongoDB if needed
        await self.log_media_to_db(user_id, session['current_file'])
        
        # Show main menu
        await update.message.reply_text(
            f"✅ Video downloaded!\n"
            f"📦 Size: {video.file_size // 1024 // 1024} MB\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu('video')
        )
    
    async def handle_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Handle incoming audio files."""
        audio = update.message.audio
        user_id = update.effective_user.id
        
        await update.message.reply_text("📥 Downloading audio...")
        
        # Download file
        file = await context.bot.get_file(audio.file_id)
        ext = '.mp3'  # Default
        if audio.mime_type:
            # Extract extension from mime type
            ext_map = {
                'audio/mpeg': '.mp3',
                'audio/wav': '.wav',
                'audio/x-wav': '.wav',
                'audio/aac': '.aac',
                'audio/flac': '.flac',
                'audio/ogg': '.ogg'
            }
            ext = ext_map.get(audio.mime_type, '.mp3')
        
        file_path = f"storage/input/{user_id}_{audio.file_id}{ext}"
        await file.download_to_drive(file_path)
        
        # Store in session
        session['current_file'] = {
            'path': file_path,
            'type': 'audio',
            'id': audio.file_id,
            'size': audio.file_size,
            'name': audio.title or f"audio_{audio.file_id[:8]}{ext}"
        }
        
        # Show audio menu
        await update.message.reply_text(
            f"✅ Audio downloaded!\n"
            f"🎵 {audio.title or 'Unknown title'}\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu('audio')
        )
    
    async def handle_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Handle document files (could be video/audio)."""
        document = update.message.document
        user_id = update.effective_user.id
        
        # Check file extension
        file_name = document.file_name or f"file_{document.file_id}"
        file_ext = os.path.splitext(file_name)[1].lower()
        
        # Determine file type
        if file_ext in self.converter.supported_formats['video']:
            file_type = 'video'
        elif file_ext in self.converter.supported_formats['audio']:
            file_type = 'audio'
        else:
            await update.message.reply_text(
                f"❌ Unsupported file format: {file_ext}\n"
                f"Supported formats:\n"
                f"Video: {', '.join(self.converter.supported_formats['video'][:5])}\n"
                f"Audio: {', '.join(self.converter.supported_formats['audio'][:5])}"
            )
            return
        
        await update.message.reply_text(f"📥 Downloading {file_type}...")
        
        # Download file
        file = await context.bot.get_file(document.file_id)
        file_path = f"storage/input/{user_id}_{document.file_id}{file_ext}"
        await file.download_to_drive(file_path)
        
        # Store in session
        session['current_file'] = {
            'path': file_path,
            'type': file_type,
            'id': document.file_id,
            'size': document.file_size,
            'name': file_name
        }
        
        # Show appropriate menu
        await update.message.reply_text(
            f"✅ {file_type.capitalize()} downloaded!\n"
            f"📁 {file_name}\n"
            f"Choose an action:",
            reply_markup=MediaMenuBuilder.get_main_menu(file_type)
        )
    
    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all callback queries with enhanced features."""
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        data = query.data
        # Accept older/alternate callback names from `MediaMenuBuilder` by mapping
        # them to the canonical names expected by this handler. This keeps
        # `utils/keyboard_utils.py` unchanged while ensuring callbacks are handled.
        aliases = {
            # Main/menu aliases
            "video_tools": "menu_video",
            "back_to_main": "menu_main",
            "media_info": "info",
            "send_file": "menu_main",
            "help": "menu_main",
            "quick_start": "menu_main",

            # Conversion / format aliases
            "convert_audio": "convert_format_menu",
            "convert_video": "convert_format_menu",
            "audio_mp3": "format_mp3",
            "audio_wav": "format_wav",
            "audio_aac": "format_aac",
            "audio_flac": "format_flac",
            "audio_ogg": "format_ogg",
            "audio_m4a": "format_m4a",

            # Merge aliases
            "merge_audio": "merge_audios_menu",
            "merge_start": "merge_videos_start",
            # individual merge menu actions are handled via UI flow; map sensible
            "merge_add": "merge_add",
            "merge_view": "merge_view",
            "merge_clear": "merge_clear",

            # Resolution presets: map explicit WxH to handler-friendly keys
            "res_3840_2160": "res_4k",
            "res_1920_1080": "res_1080",
            "res_1280_720": "res_720",
            "res_854_480": "res_480",
            "res_640_360": "res_360",

            # Screenshot menu differences
            "screenshot_grid_3": "screenshot_9grid",
            "screenshot_grid_4": "screenshot_multiple",

            # Extraction aliases
            "extract_audio_only": "extract_audio",
            "extract_video_only": "extract_streams",
            "extract_all": "extract_all_streams",

            # Misc small mappings
            "add_audio": "merge_av_menu",
        }

        # Remap data if an alias exists
        data = aliases.get(data, data)
        # Import canonical callback names for comparison when needed
        try:
            from utils.callbacks import (
                MENU_MAIN,
                MENU_VIDEO,
                INFO,
                COMPRESS_MENU,
                SCREENSHOTS_MENU,
                RESOLUTION_MENU,
                OPTIMIZE_MENU,
                MERGE_VIDEOS_START,
                MERGE_AUDIOS_START,
                BITRATE_PREFIX,
            )
        except Exception:
            MENU_MAIN = MENU_VIDEO = INFO = COMPRESS_MENU = SCREENSHOTS_MENU = RESOLUTION_MENU = OPTIMIZE_MENU = MERGE_VIDEOS_START = MERGE_AUDIOS_START = BITRATE_PREFIX = None

        # Map video bitrate shortcuts to generic bitrate handler
        if isinstance(data, str) and data.startswith('vbitrate_'):
            data = 'bitrate_' + data.split('_', 1)[1]
        
        # Ensure session exists
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {'files': {}, 'current_file': None}
        
        session = self.user_sessions[user_id]
        current_file = session.get('current_file')
        
        # Main menu navigation
        if data == "menu_main":
            await query.edit_message_text(
                "🎬 **Media Conversion Bot**\nSelect a category:",
                reply_markup=MediaMenuBuilder.get_main_menu(
                    current_file['type'] if current_file else None
                )
            )
        
        elif data == "menu_video":
            await query.edit_message_text(
                "🎬 **Video Tools**\nChoose an action:",
                reply_markup=MediaMenuBuilder.get_video_tools_menu()
            )
        
        elif data == "menu_audio":
            await query.edit_message_text(
                "🎧 **Audio Tools**\nChoose an action:",
                reply_markup=MediaMenuBuilder.get_audio_tools_menu()
            )
        
        elif data == "menu_advanced":
            await query.edit_message_text(
                "🔧 **Advanced Tools**\nChoose an action:",
                reply_markup=MediaMenuBuilder.get_advanced_tools_menu()
            )
        
        # Video tools
        elif data == "convert_mp3":
            await self.convert_to_mp3(update, context, session)
        
        elif data == "compress_menu":
            await query.edit_message_text(
                "📉 **Compression Options**\nSelect quality preset:",
                reply_markup=MediaMenuBuilder.get_compression_menu()
            )
        
        elif data.startswith("compress_"):
            crf = data.split("_")[1]
            await self.compress_video(update, context, session, crf)
        
        elif data == "trim_video":
            await query.edit_message_text(
                "✂️ **Video Trimming**\nSend start time (HH:MM:SS):\nExample: 00:01:30"
            )
            context.user_data['awaiting_trim'] = 'start'
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        
        elif data == "merge_videos_menu":
            await query.edit_message_text(
                "🔀 **Merge Videos**\nSend multiple video files, then click 'Start Merge':",
                reply_markup=MediaMenuBuilder.get_merge_menu("video")
            )
        
        elif data == "merge_videos_start":
            await self.merge_videos(update, context, session)
        
        elif data == "remove_audio":
            await self.remove_audio(update, context, session)
        
        elif data == "merge_av_menu":
            await query.edit_message_text(
                "🎵 **Merge Audio with Video**\nFirst send the audio file, then select this option again."
            )
        
        elif data == "resolution_menu":
            await query.edit_message_text(
                "📐 **Change Resolution**\nSelect preset:",
                reply_markup=MediaMenuBuilder.get_resolution_menu()
            )
        
        elif data.startswith("res_"):
            resolution = data.split("_")[1]
            await self.change_resolution(update, context, session, resolution)
        
        elif data == "optimize_menu":
            await query.edit_message_text(
                "⚡ **Optimize Video**\nSelect optimization preset:",
                reply_markup=MediaMenuBuilder.get_optimize_menu()
            )
        
        elif data.startswith("optimize_"):
            preset = data.split("_")[1]
            await self.optimize_video(update, context, session, preset)
        
        elif data == "repair_video":
            await self.repair_video(update, context, session)
        
        elif data == "screenshots_menu":
            await query.edit_message_text(
                "🖼️ **Screenshot Options**\nChoose an option:",
                reply_markup=MediaMenuBuilder.get_screenshots_menu()
            )
        
        elif data.startswith("screenshot_"):
            option = data.split("_")[1]
            await self.take_screenshot(update, context, session, option)
        
        elif data == "extract_streams":
            await self.extract_streams(update, context, session)
        
        elif data == "extract_audio":
            await self.extract_audio(update, context, session)
        
        # Audio tools
        elif data == "convert_format_menu":
            await query.edit_message_text(
                "🔄 **Convert Audio Format**\nSelect target format:",
                reply_markup=MediaMenuBuilder.get_format_menu("audio")
            )
        
        elif data.startswith("format_"):
            format_type = data.split("_")[1]
            await self.convert_audio_format(update, context, session, format_type)
        
        elif data == "bitrate_menu":
            await query.edit_message_text(
                "🎚️ **Adjust Bitrate**\nSelect bitrate:",
                reply_markup=MediaMenuBuilder.get_bitrate_menu()
            )

        # Merge list interactions
        elif data == "merge_add":
            # Add the current file to the merge list
            current_file = session.get('current_file')
            if not current_file:
                await query.edit_message_text("❌ No current file to add. Send a file first.")
                return
            path = current_file.get('path')
            if not path or not os.path.exists(path):
                await query.edit_message_text("❌ File not available to add.")
                return
            # Ensure merge_list stores file paths
            if 'merge_list' not in session:
                session['merge_list'] = []
            session['merge_list'].append(path)
            await query.edit_message_text(f"➕ Added to merge list. Total files: {len(session['merge_list'])}")

        elif data == "merge_view":
            if 'merge_list' not in session or not session['merge_list']:
                await query.edit_message_text("🗒️ Merge list is empty.")
            else:
                names = [os.path.basename(p) for p in session['merge_list']]
                await query.edit_message_text("🗒️ Files in merge list:\n" + "\n".join(names))

        elif data == "merge_clear":
            session['merge_list'] = []
            await query.edit_message_text("🗑️ Merge list cleared.")

        elif data == "framerate_menu":
            await query.edit_message_text(
                "⏱️ **Change Framerate**\nEnter target FPS (e.g., 24, 30, 60) or type '24','30','60' for presets."
            )
            context.user_data['awaiting_framerate'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]

        elif data == "fade_menu":
            await query.edit_message_text("📈 Fade In/Out: feature coming soon.")
        
        elif data == "cancel":
            # Clear any awaiting inputs and notify user
            for key in list(context.user_data.keys()):
                if key.startswith('awaiting_'):
                    del context.user_data[key]
            await query.edit_message_text(
                "❌ Operation cancelled.",
                reply_markup=MediaMenuBuilder.get_main_menu(current_file['type'] if current_file else None)
            )

        elif data == "confirm":
            await query.edit_message_text(
                "✅ Confirmed.",
                reply_markup=MediaMenuBuilder.get_main_menu(current_file['type'] if current_file else None)
            )
        
        elif data.startswith("bitrate_"):
            bitrate = data.split("_")[1]
            await self.adjust_bitrate(update, context, session, bitrate)
        
        elif data == "trim_audio":
            await query.edit_message_text(
                "✂️ **Trim Audio**\nSend start time (HH:MM:SS):"
            )
            context.user_data['awaiting_trim'] = 'start'
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        elif data == "merge_audios_menu":
            await query.edit_message_text(
                "🔀 **Merge Audio Files**\nSend multiple audio files, then click 'Start Merge':",
                reply_markup=MediaMenuBuilder.get_merge_menu("audio")
            )
        
        elif data == "merge_audios_start":
            await self.merge_audios(update, context, session)
        
        elif data == "normalize_audio":
            await self.normalize_audio(update, context, session)
        
        # Advanced tools
        elif data == "extract_all_streams":
            await self.extract_all_streams(update, context, session)
        
        elif data == "extract_subtitles":
            await self.extract_subtitles(update, context, session)
        
        elif data == "edit_metadata":
            await query.edit_message_text(
                "🏷️ **Edit Metadata**\nSend metadata as JSON:\n"
                '{"title": "New Title", "artist": "Artist Name"}'
            )
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        elif data == "full_info":
            await self.show_full_info(update, context, session)
        
        elif data == "create_archive":
            await self.create_archive(update, context, session)
        
        elif data == "batch_process":
            await query.edit_message_text(
                "🔀 **Batch Processing**\nComing soon! Send multiple files to process."
            )
        
        elif data == "thumbnail_grid":
            await self.create_thumbnail_grid(update, context, session)
        
        elif data == "generate_sample":
            await self.generate_sample(update, context, session)
        
        elif data == "add_subtitles":
            await query.edit_message_text(
                "➕ **Add Subtitles**\nSend subtitle file (.srt, .ass):"
            )
        
        elif data == "burn_subtitles":
            await query.edit_message_text(
                "✏️ **Burn Subtitles**\nComing soon!"
            )
        
        # Information
        elif data == "info":
            await self.show_media_info(update, context, session)
        
        else:
            await query.edit_message_text(f"Unknown command: {data}")
    
    # ========== IMPLEMENTATION METHODS ==========
    
    async def convert_to_mp3(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Convert video to MP3."""
        query = update.callback_query
        current_file = session.get('current_file')
        user_id = update.effective_user.id
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        # Check rate limiting
        conversion_limiter = context.application.bot_data.get('conversion_rate_limiter')
        if conversion_limiter:
            allowed, message = await conversion_limiter.can_convert(str(user_id))
            if not allowed:
                await query.edit_message_text(message)
                return
        
        # Check queue status
        active_count = len(self.active_conversions)
        semaphore_size = self.conversion_semaphore._initial_value
        max_conversions = semaphore_size + (self.conversion_semaphore._waiters.__len__() if hasattr(self.conversion_semaphore, '_waiters') else 0)
        
        if active_count >= max_conversions:
            queue_position = max_conversions - active_count + 1
            await query.edit_message_text(
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{max_conversions}\n"
                f"Your conversion will start soon..."
            )
        else:
            await query.edit_message_text("🎵 Converting to MP3...")
        
        async def do_conversion():
            # Lock the input file to prevent concurrent access
            if AsyncFileLock:
                lock = await AsyncFileLock.acquire(current_file['path'])
                async with lock:
                    output_path = f"storage/output/{current_file['id']}_audio.mp3"
                    success = await self.converter.extract_audio_from_video(
                        current_file['path'], output_path, 'mp3', '192k'
                    )
                    
                    if success and os.path.exists(output_path):
                        file_size = os.path.getsize(output_path)
                        
                        if file_size > 50 * 1024 * 1024:  # 50MB Telegram limit
                            await query.edit_message_text(
                                f"❌ File too large ({file_size//1024//1024}MB).\n"
                                "Try compression first."
                            )
                            os.remove(output_path)
                        else:
                            with open(output_path, 'rb') as audio_file:
                                await context.bot.send_audio(
                                    chat_id=update.effective_chat.id,
                                    audio=audio_file,
                                    caption="✅ Converted to MP3",
                                    title=current_file['name'].replace('.mp4', '.mp3'),
                                    performer="Media Bot"
                                )
                            os.remove(output_path)
                    else:
                        await query.edit_message_text("❌ Conversion failed.")
                    
                    await AsyncFileLock.release(current_file['path'])
            else:
                # Fallback without locking
                output_path = f"storage/output/{current_file['id']}_audio.mp3"
                success = await self.converter.extract_audio_from_video(
                    current_file['path'], output_path, 'mp3', '192k'
                )
                
                if success and os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    
                    if file_size > 50 * 1024 * 1024:  # 50MB Telegram limit
                        await query.edit_message_text(
                            f"❌ File too large ({file_size//1024//1024}MB).\n"
                            "Try compression first."
                        )
                        os.remove(output_path)
                    else:
                        with open(output_path, 'rb') as audio_file:
                            await context.bot.send_audio(
                                chat_id=update.effective_chat.id,
                                audio=audio_file,
                                caption="✅ Converted to MP3",
                                title=current_file['name'].replace('.mp4', '.mp3'),
                                performer="Media Bot"
                            )
                        os.remove(output_path)
                else:
                    await query.edit_message_text("❌ Conversion failed.")
        
        await self._run_with_concurrency_limit(user_id, "mp3_conversion", do_conversion())
    
    async def compress_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, crf: str):
        """Compress video with specified CRF."""
        query = update.callback_query
        user_id = update.effective_user.id
        
        if crf == "custom":
            await query.edit_message_text("Enter CRF value (18-51, lower=better quality):")
            context.user_data['awaiting_crf'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        current_file = session.get('current_file')
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        active_count = len(self.active_conversions)
        semaphore_size = self.conversion_semaphore._initial_value
        
        if active_count >= semaphore_size:
            queue_position = semaphore_size - active_count + 1
            await query.edit_message_text(
                f"⏳ Queue position: #{queue_position}\n"
                f"Active conversions: {active_count}/{semaphore_size}\n"
                f"Your compression will start soon..."
            )
        else:
            await query.edit_message_text(f"📉 Compressing with CRF {crf}...")
        
        async def do_compression():
            output_path = f"storage/output/{current_file['id']}_compressed.mp4"
            
            # Map resolution presets
            resolution_map = {
                '4k_to_1080': ('1920', '1080'),
                '1080_to_720': ('1280', '720')
            }
            
            if crf in resolution_map:
                width, height = resolution_map[crf]
                success = await self.converter.change_resolution(
                    current_file['path'], output_path, int(width), int(height)
                )
            else:
                success = await self.converter.optimize_video(
                    current_file['path'], output_path, 'medium', int(crf) if crf.isdigit() else 28
                )
            
            if success and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                
                if file_size > 2 * 1024**3:  # 2GB
                    await query.edit_message_text(
                        f"❌ Compressed file still too large ({file_size//1024//1024}MB).\n"
                        "Try higher compression."
                    )
                    os.remove(output_path)
                else:
                    with open(output_path, 'rb') as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Compressed (CRF {crf})",
                            supports_streaming=True
                        )
                    os.remove(output_path)
            else:
                await query.edit_message_text("❌ Compression failed.")
        
        await self._run_with_concurrency_limit(user_id, "compression", do_compression())
    
    async def merge_videos(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Merge multiple videos."""
        query = update.callback_query
        
        if 'merge_list' not in session or len(session['merge_list']) < 2:
            await query.edit_message_text(
                "❌ Need at least 2 videos to merge.\n"
                "Send video files first, then click 'Start Merge'."
            )
            return
        
        await query.edit_message_text(f"🔀 Merging {len(session['merge_list'])} videos...")
        
        output_path = f"storage/output/merged_{int(datetime.now().timestamp())}.mp4"
        success = await self.converter.merge_videos(session['merge_list'], output_path)
        
        if success and os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            
            with open(output_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Merged {len(session['merge_list'])} videos",
                    supports_streaming=True
                )
            
            # Cleanup
            os.remove(output_path)
            for file_path in session['merge_list']:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session['merge_list'] = []
        else:
            await query.edit_message_text("❌ Merge failed.")
    
    async def merge_audios(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Merge multiple audio files."""
        query = update.callback_query
        
        if 'merge_list' not in session or len(session['merge_list']) < 2:
            await query.edit_message_text(
                "❌ Need at least 2 audio files to merge.\n"
                "Send audio files first, then click 'Start Merge'."
            )
            return
        
        await query.edit_message_text(f"🔀 Merging {len(session['merge_list'])} audio files...")
        
        output_path = f"storage/output/merged_{int(datetime.now().timestamp())}.mp3"
        success = await self.converter.merge_audios(session['merge_list'], output_path)
        
        if success and os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            
            with open(output_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Merged {len(session['merge_list'])} audio files",
                    title="Merged Audio"
                )
            
            # Cleanup
            os.remove(output_path)
            for file_path in session['merge_list']:
                if os.path.exists(file_path):
                    os.remove(file_path)
            session['merge_list'] = []
        else:
            await query.edit_message_text("❌ Merge failed.")
    
    async def remove_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Remove audio from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("🔉 Removing audio...")
        
        output_path = f"storage/output/{current_file['id']}_no_audio.mp4"
        success = await self.converter.remove_audio(current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption="✅ Audio removed",
                    supports_streaming=True
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to remove audio.")
    
    async def change_resolution(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, resolution: str):
        """Change video resolution."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        # Resolution mapping
        res_map = {
            '4k': (3840, 2160),
            '2k': (2560, 1440),
            '1080': (1920, 1080),
            '720': (1280, 720),
            '480': (854, 480),
            '360': (640, 360),
            'mobile': (480, 854)  # Portrait mobile
        }
        
        if resolution == 'custom':
            await query.edit_message_text("Enter resolution (WIDTHxHEIGHT):\nExample: 1280x720")
            context.user_data['awaiting_resolution'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        if resolution not in res_map:
            await query.edit_message_text("❌ Invalid resolution.")
            return
        
        width, height = res_map[resolution]
        await query.edit_message_text(f"📐 Changing resolution to {width}x{height}...")
        
        output_path = f"storage/output/{current_file['id']}_{width}x{height}.mp4"
        success = await self.converter.change_resolution(
            current_file['path'], output_path, width, height
        )
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Resolution: {width}x{height}",
                    supports_streaming=True
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to change resolution.")
    
    async def optimize_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, preset: str):
        """Optimize video for specific use case."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        # Preset mapping
        preset_map = {
            'web': ('slow', 23, '128k'),
            'mobile': ('medium', 28, '96k'),
            'tv': ('slow', 20, '192k'),
            'storage': ('veryfast', 35, '64k'),
            'fast': ('veryfast', 28, '128k')
        }
        
        if preset == 'custom':
            await query.edit_message_text(
                "Enter optimization settings:\n"
                "Format: preset,crf,bitrate\n"
                "Example: slow,23,128k"
            )
            context.user_data['awaiting_optimize'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        if preset not in preset_map:
            await query.edit_message_text("❌ Invalid preset.")
            return
        
        encoder_preset, crf, bitrate = preset_map[preset]
        await query.edit_message_text(f"⚡ Optimizing for {preset}...")
        
        output_path = f"storage/output/{current_file['id']}_optimized.mp4"
        
        # Use FFmpeg command for optimization
        cmd = [
            '-c:v', 'libx264',
            '-preset', encoder_preset,
            '-crf', str(crf),
            '-c:a', 'aac',
            '-b:a', bitrate,
            '-movflags', '+faststart'
        ]
        
        success, _ = await self.converter.execute_ffmpeg(cmd, current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption=f"✅ Optimized for {preset}",
                    supports_streaming=True
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Optimization failed.")
    
    async def repair_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Attempt to repair corrupted video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("🔧 Attempting to repair video...")
        
        output_path = f"storage/output/{current_file['id']}_repaired.mp4"
        success = await self.converter.repair_video(current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as video_file:
                await context.bot.send_video(
                    chat_id=update.effective_chat.id,
                    video=video_file,
                    caption="✅ Video repaired (if possible)",
                    supports_streaming=True
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Repair failed or not needed.")
    
    async def take_screenshot(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, option: str):
        """Take screenshot(s) from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        # Get video duration for calculations (try ffmpeg-python binding, fallback to error)
        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib
                ffmpeg_mod = importlib.import_module('ffmpeg')
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await query.edit_message_text(
                "FFmpeg-python binding is not available on the server. This operation requires ffmpeg-python."
            )
            logger.info("ffmpeg-python not available for take_screenshot; falling back to CLI where possible")
            return

        try:
            probe = ffmpeg_mod.probe(current_file['path'])
            duration = float(probe['format']['duration'])
        except Exception as e:
            logger.warning(f"ffmpeg.probe failed: {e}")
            await query.edit_message_text("Failed to read media info for screenshot operation.")
            return
        
        if option == 'custom':
            await query.edit_message_text("Enter time (HH:MM:SS or seconds):")
            context.user_data['awaiting_screenshot_time'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        # Calculate time based on option
        time_map = {
            'start': '00:00:01',
            'middle': f"{int(duration/2//3600):02d}:{int((duration/2%3600)//60):02d}:{duration/2%60:06.3f}",
            'end': f"{int((duration-1)//3600):02d}:{int(((duration-1)%3600)//60):02d}:{(duration-1)%60:06.3f}"
        }
        
        if option == '9grid':
            await self.create_thumbnail_grid(update, context, session)
            return
        elif option == 'multiple':
            await query.edit_message_text("How many screenshots? (2-20)")
            context.user_data['awaiting_screenshot_count'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        time_str = time_map.get(option, '00:00:01')
        await query.edit_message_text(f"🖼️ Taking screenshot at {time_str}...")
        
        output_path = f"storage/output/{current_file['id']}_screenshot.jpg"
        success = await self.converter.take_screenshot_at_time(
            current_file['path'], output_path, time_str
        )
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption=f"✅ Screenshot at {time_str}"
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to take screenshot.")
    
    async def create_thumbnail_grid(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Create thumbnail grid from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("🖼️ Creating thumbnail grid...")
        
        output_path = f"storage/output/{current_file['id']}_grid.jpg"
        success = await self.converter.extract_thumbnail_grid(
            current_file['path'], output_path, 3, 3
        )
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as photo_file:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_file,
                    caption="✅ Thumbnail grid (3x3)"
                )
            os.remove(output_path)
        else:
            # Fallback to single screenshot
            await self.take_screenshot(update, context, session, 'middle')
    
    async def extract_streams(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Extract all streams from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("🎞️ Extracting streams...")
        
        output_dir = f"storage/output/{current_file['id']}_streams"
        os.makedirs(output_dir, exist_ok=True)
        
        extracted = await self.converter.extract_streams(current_file['path'], output_dir)
        
        if extracted:
            # Create archive of extracted streams
            archive_path = f"{output_dir}.zip"
            await self.converter.create_archive(list(extracted.values()), archive_path)
            
            if os.path.exists(archive_path):
                with open(archive_path, 'rb') as archive_file:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=archive_file,
                        caption=f"✅ Extracted {len(extracted)} streams"
                    )
                
                # Cleanup
                os.remove(archive_path)
                for file_path in extracted.values():
                    if os.path.exists(file_path):
                        os.remove(file_path)
                os.rmdir(output_dir)
            else:
                await query.edit_message_text("✅ Streams extracted to folder.")
        else:
            await query.edit_message_text("❌ No streams found or extraction failed.")
    
    async def extract_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Extract audio from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("🎵 Extracting audio...")
        
        output_path = f"storage/output/{current_file['id']}_audio.mp3"
        success = await self.converter.extract_audio_from_video(
            current_file['path'], output_path, 'mp3', '192k'
        )
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption="✅ Audio extracted",
                    title=f"{current_file['name']}_audio"
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to extract audio.")
    
    async def convert_audio_format(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, format_type: str):
        """Convert audio to different format."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'audio':
            await query.edit_message_text("❌ No audio file found.")
            return
        
        await query.edit_message_text(f"🔄 Converting to {format_type.upper()}...")
        
        output_path = f"storage/output/{current_file['id']}_converted.{format_type}"
        success = await self.converter.convert_audio_format(
            current_file['path'], output_path, format_type
        )
        
        if success and os.path.exists(output_path):
            mime_type = {
                'mp3': 'audio/mpeg',
                'wav': 'audio/wav',
                'aac': 'audio/aac',
                'flac': 'audio/flac',
                'ogg': 'audio/ogg',
                'm4a': 'audio/mp4'
            }.get(format_type, 'audio/mpeg')
            
            with open(output_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Converted to {format_type.upper()}",
                    title=f"{os.path.splitext(current_file['name'])[0]}.{format_type}",
                    mime_type=mime_type
                )
            os.remove(output_path)
        else:
            await query.edit_message_text(f"❌ Failed to convert to {format_type}.")
    
    async def adjust_bitrate(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict, bitrate: str):
        """Adjust audio bitrate."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'audio':
            await query.edit_message_text("❌ No audio file found.")
            return
        
        if bitrate == 'custom':
            await query.edit_message_text("Enter bitrate (e.g., 128k, 320k):")
            context.user_data['awaiting_bitrate'] = True
            for key in list(context.user_data.keys()):
                if key.startswith("awaiting_"):
                    del context.user_data[key]
        
        await query.edit_message_text(f"🎚️ Setting bitrate to {bitrate}...")
        
        output_path = f"storage/output/{current_file['id']}_{bitrate}.mp3"
        
        # Convert with specific bitrate
        cmd = ['-c:a', 'libmp3lame', '-b:a', bitrate]
        
        success, _ = await self.converter.execute_ffmpeg(cmd, current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption=f"✅ Bitrate: {bitrate}",
                    title=f"{current_file['name']}_{bitrate}"
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to adjust bitrate.")
    
    async def normalize_audio(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Normalize audio volume."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'audio':
            await query.edit_message_text("❌ No audio file found.")
            return
        
        await query.edit_message_text("🔊 Normalizing audio...")
        
        output_path = f"storage/output/{current_file['id']}_normalized.mp3"
        
        # Use loudnorm filter for normalization
        cmd = [
            '-filter:a', 'loudnorm=I=-16:TP=-1.5:LRA=11',
            '-c:a', 'libmp3lame',
            '-b:a', '192k'
        ]
        
        success, _ = await self.converter.execute_ffmpeg(cmd, current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    caption="✅ Audio normalized",
                    title=f"{current_file['name']}_normalized"
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ Failed to normalize audio.")
    
    async def extract_all_streams(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Extract all streams (video, audio, subtitles)."""
        await self.extract_streams(update, context, session)
    
    async def extract_subtitles(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Extract subtitles from video."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file or current_file['type'] != 'video':
            await query.edit_message_text("❌ No video file found.")
            return
        
        await query.edit_message_text("📝 Extracting subtitles...")
        
        output_path = f"storage/output/{current_file['id']}_subtitles.srt"
        success = await self.converter.extract_subtitles(current_file['path'], output_path)
        
        if success and os.path.exists(output_path):
            with open(output_path, 'rb') as sub_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=sub_file,
                    caption="✅ Subtitles extracted",
                    filename=f"{current_file['name']}_subtitles.srt"
                )
            os.remove(output_path)
        else:
            await query.edit_message_text("❌ No subtitles found or extraction failed.")
    
    async def show_full_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Show full media information."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file:
            await query.edit_message_text("❌ No file found.")
            return
        
        await query.edit_message_text("📊 Analyzing media...")
        
        ffmpeg_mod = ffmpeg
        if ffmpeg_mod is None:
            try:
                import importlib
                ffmpeg_mod = importlib.import_module('ffmpeg')
            except Exception:
                ffmpeg_mod = None

        if not ffmpeg_mod:
            await query.edit_message_text(
                "FFmpeg-python binding is not available on the server. Full media analysis requires ffmpeg-python."
            )
            logger.warning("ffmpeg-python not available for media analysis")
            return

        try:
            probe = ffmpeg_mod.probe(current_file['path'])
            
            # Format information
            format_info = probe.get('format', {})
            streams = probe.get('streams', [])
            
            info_text = f"📊 **Full Media Analysis**\n\n"
            info_text += f"📁 **File:** {current_file['name']}\n"
            info_text += f"📦 **Size:** {format_info.get('size', 0) // 1024 // 1024} MB\n"
            info_text += f"🎞️ **Format:** {format_info.get('format_name', 'N/A')}\n"
            info_text += f"⏱️ **Duration:** {float(format_info.get('duration', 0)):.2f}s\n"
            info_text += f"📈 **Bitrate:** {int(format_info.get('bit_rate', 0)) // 1000} kbps\n\n"
            
            # Streams information
            info_text += f"🎬 **Streams ({len(streams)}):**\n"
            
            for i, stream in enumerate(streams):
                codec_type = stream.get('codec_type', 'unknown')
                info_text += f"\n**Stream {i+1} ({codec_type}):**\n"
                
                if codec_type == 'video':
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Resolution: {stream.get('width', 'N/A')}x{stream.get('height', 'N/A')}\n"
                    num, den = stream.get('avg_frame_rate', '0/1').split('/')
                    fps = float(num) / float(den) if float(den) != 0 else 0
                    info_text += f"  Bitrate: {int(stream.get('bit_rate', 0)) // 1000 if stream.get('bit_rate') else 'N/A'} kbps\n"
                
                elif codec_type == 'audio':
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Channels: {stream.get('channels', 'N/A')}\n"
                    info_text += f"  Sample Rate: {stream.get('sample_rate', 'N/A')} Hz\n"
                    info_text += f"  Bitrate: {int(stream.get('bit_rate', 0)) // 1000 if stream.get('bit_rate') else 'N/A'} kbps\n"
                
                elif codec_type == 'subtitle':
                    info_text += f"  Codec: {stream.get('codec_name', 'N/A')}\n"
                    info_text += f"  Language: {stream.get('tags', {}).get('language', 'N/A')}\n"
            
            await query.edit_message_text(info_text[:4000])  # Telegram message limit
            
        except Exception as e:
            logger.error(f"Error analyzing media: {e}")
            await query.edit_message_text("❌ Failed to analyze media.")
    
    async def create_archive(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Create archive of processed files."""
        query = update.callback_query
        
        # Get all files in output directory for this user
        user_id = update.effective_user.id
        output_dir = "storage/output"
        user_files = [f for f in os.listdir(output_dir) if f.startswith(str(user_id))]
        
        if not user_files:
            await query.edit_message_text("❌ No files to archive.")
            return
        
        await query.edit_message_text("📦 Creating archive...")
        
        # Create list of full file paths
        file_paths = [os.path.join(output_dir, f) for f in user_files]
        archive_path = f"storage/output/{user_id}_archive.zip"
        
        success = await self.converter.create_archive(file_paths, archive_path)
        
        if success and os.path.exists(archive_path):
            with open(archive_path, 'rb') as archive_file:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=archive_file,
                    caption=f"✅ Archive of {len(user_files)} files"
                )
            
            # Cleanup
            os.remove(archive_path)
        else:
            await query.edit_message_text("❌ Failed to create archive.")
    
    async def generate_sample(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Generate sample/preview of media."""
        query = update.callback_query
        current_file = session.get('current_file')
        
        if not current_file:
            await query.edit_message_text("❌ No file found.")
            return
        
        await query.edit_message_text("🎬 Generating 30-second sample...")
        
        output_path = f"storage/output/{current_file['id']}_sample"
        if current_file['type'] == 'video':
            output_path += '.mp4'
            success = await self.converter.generate_sample(current_file['path'], output_path, 30)
            
            if success and os.path.exists(output_path):
                with open(output_path, 'rb') as video_file:
                    await context.bot.send_video(
                        chat_id=update.effective_chat.id,
                        video=video_file,
                        caption="✅ 30-second sample",
                        supports_streaming=True
                    )
                os.remove(output_path)
            else:
                await query.edit_message_text("❌ Failed to generate sample.")
        
        elif current_file['type'] == 'audio':
            output_path += '.mp3'
            # Extract first 30 seconds
            cmd = ['-t', '30', '-c', 'copy']
            
            success, _ = await self.converter.execute_ffmpeg(cmd, current_file['path'], output_path)
            
            if success and os.path.exists(output_path):
                with open(output_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=update.effective_chat.id,
                        audio=audio_file,
                        caption="✅ 30-second sample",
                        title=f"{current_file['name']}_sample"
                    )
                os.remove(output_path)
            else:
                await query.edit_message_text("❌ Failed to generate sample.")
    
    async def show_media_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: Dict):
        """Show basic media information."""
        await self.show_full_info(update, context, session)
    
    async def log_media_to_db(self, user_id: int, file_info: Dict):
        """Log media processing to MongoDB."""
        try:
            if MediaConversionModel is None:
                logger.info("MongoDB model not available; skipping DB logging")
                return

            # If a MongoDB URL is not configured, skip logging
            import os
            mongo_url = os.environ.get('MONGODB_URL')
            if not mongo_url:
                logger.info('MONGODB_URL not set; skipping DB logging')
                return

            # Lazy-import motor and initialize model
            try:
                from motor.motor_asyncio import AsyncIOMotorClient
                client = AsyncIOMotorClient(mongo_url)
                db_model = MediaConversionModel(client, db_name=os.environ.get('MONGODB_NAME', None) or 'media_conversion_bot')

                log_entry = {
                    'user_id': user_id,
                    'file_name': file_info['name'],
                    'file_type': file_info['type'],
                    'file_size': file_info['size'],
                    'timestamp': datetime.now(),
                    'action': 'upload'
                }

                await db_model.log_conversion(log_entry)
                logger.info(f"Logged media upload for user {user_id}")
            except Exception as e:
                logger.error(f"Failed to log to MongoDB: {e}")
        except Exception as e:
            logger.error(f"Failed to log to MongoDB: {e}")
    
    async def handle_custom_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle custom user input for various operations."""
        user_input = update.message.text.strip()
        user_id = update.effective_user.id
        
        if user_id not in self.user_sessions:
            await update.message.reply_text("❌ Session expired. Please send a file first.")
            return ConversationHandler.END
        
        session = self.user_sessions[user_id]
        current_file = session.get('current_file')
        
        # Check what we're waiting for
        if context.user_data.get('awaiting_crf'):
            if user_input.isdigit() and 18 <= int(user_input) <= 51:
                await self.compress_video(update, context, session, user_input)
            else:
                await update.message.reply_text("❌ Invalid CRF. Enter 18-51.")
                for key in list(context.user_data.keys()):
                    if key.startswith("awaiting_"):
                        del context.user_data[key]
        
        elif context.user_data.get('awaiting_resolution'):
            if 'x' in user_input:
                try:
                    width, height = map(int, user_input.split('x'))
                    await update.message.reply_text(f"📐 Changing resolution to {width}x{height}...")
                    
                    output_path = f"storage/output/{current_file['id']}_{width}x{height}.mp4"
                    success = await self.converter.change_resolution(
                        current_file['path'], output_path, width, height
                    )
                    
                    if success and os.path.exists(output_path):
                        with open(output_path, 'rb') as video_file:
                            await context.bot.send_video(
                                chat_id=update.effective_chat.id,
                                video=video_file,
                                caption=f"✅ Resolution: {width}x{height}",
                                supports_streaming=True
                            )
                        os.remove(output_path)
                    else:
                        await update.message.reply_text("❌ Failed to change resolution.")
                except:
                    await update.message.reply_text("❌ Invalid format. Use WIDTHxHEIGHT.")
                    for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
            else:
                await update.message.reply_text("❌ Invalid format. Use WIDTHxHEIGHT.")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        elif context.user_data.get('awaiting_trim'):
            # Handle trim time input
            context.user_data['trim_time'] = user_input
            if context.user_data['awaiting_trim'] == 'start':
                context.user_data['start_time'] = user_input
                context.user_data['awaiting_trim'] = 'end'
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
            else:
                # Perform trim
                start_time = context.user_data.get('start_time', '00:00:00')
                end_time = user_input
                
                await update.message.reply_text(f"✂️ Trimming from {start_time} to {end_time}...")
                
                output_path = f"storage/output/{current_file['id']}_trimmed.mp4"
                success = await self.converter.trim_video(
                    current_file['path'], output_path, start_time, end_time
                )
                
                if success and os.path.exists(output_path):
                    with open(output_path, 'rb') as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Trimmed {start_time}-{end_time}",
                            supports_streaming=True
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to trim video.")
        
        elif context.user_data.get('awaiting_screenshot_time'):
            # Handle screenshot time
            await update.message.reply_text(f"🖼️ Taking screenshot at {user_input}...")
            
            output_path = f"storage/output/{current_file['id']}_screenshot.jpg"
            success = await self.converter.take_screenshot_at_time(
                current_file['path'], output_path, user_input
            )
            
            if success and os.path.exists(output_path):
                with open(output_path, 'rb') as photo_file:
                    await context.bot.send_photo(
                        chat_id=update.effective_chat.id,
                        photo=photo_file,
                        caption=f"✅ Screenshot at {user_input}"
                    )
                os.remove(output_path)
            else:
                await update.message.reply_text("❌ Failed to take screenshot.")
        
        elif context.user_data.get('awaiting_screenshot_count'):
            # Handle multiple screenshots
            if user_input.isdigit() and 2 <= int(user_input) <= 20:
                count = int(user_input)
                await update.message.reply_text(f"🖼️ Taking {count} screenshots...")
                
                screenshots = await self.converter.take_screenshot_grid(
                    current_file['path'], f"storage/output/{current_file['id']}_grid", count
                )
                
                if screenshots:
                    # Send as album
                    media_group = []
                    for i, screenshot_path in enumerate(screenshots):
                        with open(screenshot_path, 'rb') as photo_file:
                            media_group.append(InputMediaPhoto(photo_file, caption=f"Screenshot {i+1}" if i == 0 else ""))
                    
                    await context.bot.send_media_group(
                        chat_id=update.effective_chat.id,
                        media=media_group
                    )
                    
                    # Cleanup
                    for screenshot_path in screenshots:
                        if os.path.exists(screenshot_path):
                            os.remove(screenshot_path)
                else:
                    await update.message.reply_text("❌ Failed to take screenshots.")
            else:
                await update.message.reply_text("❌ Enter number 2-20.")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        elif context.user_data.get('awaiting_framerate'):
            # Handle custom framerate input
            try:
                fps = float(user_input)
                await update.message.reply_text(f"⏱️ Changing framerate to {fps} fps...")
                output_path = f"storage/output/{current_file['id']}_fr_{int(fps)}.mp4"
                success = await self.converter.change_framerate(
                    current_file['path'], output_path, fps
                )

                if success and os.path.exists(output_path):
                    with open(output_path, 'rb') as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Framerate changed to {fps} fps",
                            supports_streaming=True
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to change framerate.")
            except Exception:
                await update.message.reply_text("❌ Invalid FPS value. Use a number like 24 or 29.97.")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        elif context.user_data.get('awaiting_bitrate'):
            # Handle custom bitrate
            if user_input.endswith('k'):
                await self.adjust_bitrate(update, context, session, user_input)
            else:
                await update.message.reply_text("❌ Invalid bitrate. Use format like 128k, 320k.")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        elif context.user_data.get('awaiting_optimize'):
            # Handle custom optimization
            try:
                preset, crf, bitrate = user_input.split(',')
                await update.message.reply_text(f"⚡ Optimizing with preset={preset}, crf={crf}, bitrate={bitrate}...")
                
                output_path = f"storage/output/{current_file['id']}_optimized.mp4"
                cmd = [
                    '-c:v', 'libx264',
                    '-preset', preset.strip(),
                    '-crf', crf.strip(),
                    '-c:a', 'aac',
                    '-b:a', bitrate.strip(),
                    '-movflags', '+faststart'
                ]
                
                success, _ = await self.converter.execute_ffmpeg(cmd, current_file['path'], output_path)
                
                if success and os.path.exists(output_path):
                    with open(output_path, 'rb') as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption=f"✅ Custom optimization",
                            supports_streaming=True
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Optimization failed.")
            except:
                await update.message.reply_text("❌ Invalid format. Use: preset,crf,bitrate")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        elif context.user_data.get('awaiting_metadata'):
            # Handle metadata JSON
            try:
                metadata = json.loads(user_input)
                output_path = f"storage/output/{current_file['id']}_with_metadata.mp4"
                
                success = await self.converter.edit_metadata(
                    current_file['path'], output_path, metadata
                )
                
                if success and os.path.exists(output_path):
                    with open(output_path, 'rb') as video_file:
                        await context.bot.send_video(
                            chat_id=update.effective_chat.id,
                            video=video_file,
                            caption="✅ Metadata updated",
                            supports_streaming=True
                        )
                    os.remove(output_path)
                else:
                    await update.message.reply_text("❌ Failed to update metadata.")
            except json.JSONDecodeError:
                await update.message.reply_text("❌ Invalid JSON format.")
                for key in list(context.user_data.keys()):
                        if key.startswith("awaiting_"):
                            del context.user_data[key]
        
        # Clear context
        for key in list(context.user_data.keys()):
            if key.startswith('awaiting_'):
                del context.user_data[key]
        
        return ConversationHandler.END
