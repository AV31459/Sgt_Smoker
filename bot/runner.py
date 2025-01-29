import asyncio
import logging
import os
import signal
import sys
from logging.config import dictConfig
from pathlib import Path

import settings
from dotenv import load_dotenv
from smokerbot import SmokerBotHandler
from telethon import TelegramClient, events


def bot_runner():
    """Запуск бота."""

    load_dotenv()

    # Настраиваем логгер
    logger = logging.getLogger('smokerbot')
    dictConfig(settings.LOG_CONFIG)
    logger.setLevel(os.getenv('APP_LOG_LEVEL', 'INFO'))
    logger.info('Smokerbot is being started...')

    # Коннектим Телеграм клиент
    client = TelegramClient(
        Path(os.getenv('DATA_PATH')) / 'smokerbot.session',
        os.getenv('CLIENT_API_ID'),
        os.getenv('CLIENT_API_HASH'),
        # device_model=os.getenv('CLIENT_DEVICE_MODEL'),
        # system_version=os.getenv('CLIENT_SYSTEM_VERSION'),
        app_version=os.getenv('CLIENT_APP_VERSION'),
        # lang_code=os.getenv('CLIENT_LANG_CODE'),
        # system_lang_code=os.getenv('CLIENT_SYSTEM_LANG_CODE'),
        connection_retries=int(os.getenv('CLIENT_CONNECTION_RETRIES'))
    )
    client.session.set_dc(
        int(os.getenv('CLIENT_DC')),
        os.getenv('CLIENT_PROD_SERVER'),
        int(os.getenv('CLIENT_PROD_PORT'))
    )
    client.start(bot_token=os.getenv('BOT_TOKEN'))

    # Настройка авто-обработки FloodWaitError модулем Telethon
    if threshold := os.getenv('CLIENT_FLOOD_SLEEP_THRESHOLD'):
        client.flood_sleep_threshold = int(threshold)

    # Создаем основной bot handler
    handler = SmokerBotHandler(
        client,
        logger,
        data_path=Path(os.getenv('DATA_PATH')),
        admin_ids=[int(os.getenv('ADMIN_USER_ID'))],
        persistence_interval=int(os.getenv('PERSISTENCE_INTERVAL', 600))
    )

    # Регистрируем обработчики событий
    client.add_event_handler(
        handler.on_new_message,
        events.NewMessage(incoming=True, func=handler.filter_event)
    )
    client.add_event_handler(
        handler.on_callback_query,
        events.CallbackQuery(func=handler.filter_event)
    )

    # Получаем loop и регистрируем сигналы
    # https://www.roguelynn.com/words/asyncio-graceful-shutdowns/
    loop = client.loop

    async def service_shutdown(signal):
        """Service shutdown on OS signal."""

        logger.info(
            f'[ main ]: Received signal {signal.name}, stopping the service'
        )

        await handler.shutdown()

        await client.disconnect()

        # Double check
        if client.is_connected():
            logger.error('[ main ]: Failed to disconnect client gracefilly')
            return loop.stop()

        logger.info('[ main ]: Client is disconnected')

    if sys.platform != 'win32':
        for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(service_shutdown(s))
            )

    try:
        logger.info('[ main ]: Running main asyncio loop')
        loop.run_until_complete(client.disconnected)

    # In case we're on win platform
    except KeyboardInterrupt:
        loop.run_until_complete(service_shutdown(signal.SIGINT))

    except Exception as exc:
        logger.error(
            'Unhandled exception in main loop: 🟥 '
            f'{exc.__class__.__name__}: {exc}', exc_info=True
        )
    finally:
        loop.stop()
        logger.info('[ main ]: Service is down')


if __name__ == '__main__':
    bot_runner()
