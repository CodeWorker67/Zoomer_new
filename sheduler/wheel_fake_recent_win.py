from logging_config import logger
from services.wheel import wheel_fake_recent_win_cron


async def wheel_fake_recent_win_job() -> None:
    try:
        await wheel_fake_recent_win_cron()
    except Exception as e:
        logger.exception("Wheel fake recent win cron failed: {}", e)
