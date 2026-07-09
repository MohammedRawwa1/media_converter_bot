#!/usr/bin/env python3
"""Final cleanup: Redis lock, dead code removal, unused import cleanup."""

# ===== PART 1: main.py - Add Redis lock =====
with open('main.py', 'r', encoding='utf-8') as f:
    content = f.read()

orig_len = len(content)
changes = 0

# 1a. Add Redis lock init before _longpoll_loop
old1 = '                logger.info("Starting background long-poller (FORCE_POLLING enabled)")\n\n                async def _longpoll_loop():'
new1 = (
    '                logger.info("Starting background long-poller (FORCE_POLLING enabled)")\n'
    '\n'
    '                # Distributed lock to prevent multiple workers from polling simultaneously\n'
    '                _longpoll_redis_lock = None\n'
    '                try:\n'
    '                    from utils.redis_lock import RedisLock\n'
    '                    _longpoll_redis_lock = RedisLock("longpoller", ttl=35)\n'
    '                except Exception:\n'
    '                    _longpoll_redis_lock = None\n'
    '\n'
    '                async def _longpoll_loop():'
)
if old1 in content:
    content = content.replace(old1, new1, 1)
    changes += 1
    print('1a. Redis lock init added')

# 1b. Add lock acquire inside _longpoll_loop
old1b = '                async def _longpoll_loop():\n                    offset = None\n                    bot = application.bot\n                    while True:\n                        try:\n                            # Use a modest timeout so we can react to shutdown_event'
new1b = (
    '                async def _longpoll_loop():\n'
    '                    offset = None\n'
    '                    bot = application.bot\n'
    '                    while True:\n'
    '                        try:\n'
    '                            # Acquire distributed lock (only one worker polls at a time)\n'
    '                            if _longpoll_redis_lock is not None:\n'
    '                                if not await _longpoll_redis_lock.acquire():\n'
    '                                    logger.debug("Long-poller: another worker holds the lock; sleeping")\n'
    '                                    await asyncio.sleep(5)\n'
    '                                    continue\n'
    '                                try:\n'
    '                                    await _longpoll_redis_lock.renew()\n'
    '                                except Exception:\n'
    '                                    pass\n'
    '                            # Use a modest timeout so we can react to shutdown_event'
)
if old1b in content:
    content = content.replace(old1b, new1b, 1)
    changes += 1
    print('1b. Lock acquire added')

# 1c. Replace Conflict handler
old1c = '                        except Conflict as e:\n                            logger.error("Long-poller conflict (another getUpdates active): %s. Stopping long-poller", e)\n                            break'
new1c = (
    '                        except Conflict as e:\n'
    '                            logger.warning("Long-poller conflict: %s. Releasing lock and retrying", e)\n'
    '                            if _longpoll_redis_lock is not None:\n'
    '                                try:\n'
    '                                    await _longpoll_redis_lock.release()\n'
    '                                except Exception:\n'
    '                                    pass\n'
    '                            await asyncio.sleep(10)\n'
    '                            continue'
)
if old1c in content:
    content = content.replace(old1c, new1c, 1)
    changes += 1
    print('1c. Conflict handler updated')

# 1d. Add finally block with lock renew
old1d = '                        except Exception as e:\n                            logger.exception(f"Long-poller error: {e}")\n                            await asyncio.sleep(1)\n\n                try:\n                    global LONG_POLLER_STARTED'
new1d = (
    '                        except Exception as e:\n'
    '                            logger.exception(f"Long-poller error: {e}")\n'
    '                            await asyncio.sleep(1)\n'
    '                        finally:\n'
    '                            if _longpoll_redis_lock is not None and _longpoll_redis_lock.is_acquired:\n'
    '                                try:\n'
    '                                    await _longpoll_redis_lock.renew()\n'
    '                                except Exception:\n'
    '                                    pass\n'
    '\n'
    '                try:\n'
    '                    global LONG_POLLER_STARTED'
)
if old1d in content:
    content = content.replace(old1d, new1d, 1)
    changes += 1
    print('1d. finally block with lock renew added')

# 1e. Update LONG_POLLER_STARTED guard
old1e = '                    if not globals().get("LONG_POLLER_STARTED", False):\n                        globals()["LONG_POLLER_STARTED"] = True\n                        polling_task = asyncio.create_task(_longpoll_loop())\n                    else:\n                        logger.info("Background long-poller already running; skipping duplicate start")'
new1e = (
    '                    can_start = True\n'
    '                    if _longpoll_redis_lock is not None:\n'
    '                        can_start = await _longpoll_redis_lock.acquire()\n'
    '                    if can_start and not globals().get("LONG_POLLER_STARTED", False):\n'
    '                        globals()["LONG_POLLER_STARTED"] = True\n'
    '                        polling_task = asyncio.create_task(_longpoll_loop())\n'
    '                    elif globals().get("LONG_POLLER_STARTED", False):\n'
    '                        logger.info("Background long-poller already running; skipping duplicate start")\n'
    '                    else:\n'
    '                        logger.info("Another worker holds long-poller lock; skipping")'
)
if old1e in content:
    content = content.replace(old1e, new1e, 1)
    changes += 1
    print('1e. LONG_POLLER_STARTED guard updated')

# 1f. Remove dead code: duplicate password handler block
# Find the dead code block after "return" in the password handler
dead_start = '            return\n\n            try:\n                await client.sign_in(password=password)\n                if await client.is_user_authorized():'
if dead_start in content:
    # Find the end of the dead block
    dead_end = '                    _clear_login_flow(user_id, context)\n            return'
    # Find the FIRST occurrence (the working one) vs the SECOND (dead)
    first_idx = content.find(dead_start)
    if first_idx >= 0:
        # Find the dead block after the first return
        dead_block_start = content.find(dead_start, first_idx + 10)
        if dead_block_start < 0:
            # The dead block is the one after the first return in password handler
            # Look for the pattern: return followed by try block that's unreachable
            pass
        else:
            # Remove from dead_block_start to end of dead block
            dead_block_end = content.find('            return', dead_block_start + 10)
            if dead_block_end > 0:
                dead_block_end += len('            return')
                content = content[:dead_block_start] + content[dead_block_end:]
                changes += 1
                print('1f. Dead code block removed')

with open('main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'\nmain.py: {changes} changes ({orig_len} -> {len(content)} chars)')
