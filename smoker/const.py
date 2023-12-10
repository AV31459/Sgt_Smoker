from telegram import BotCommand

INITIAL_USER_DATA = {
    'is_running': False,
    'interval': 60,
    'initial_sig': 1,
    'tz_offset': 3,
    'sig_available': 0,
    'ran_at': None,
    'sig_smoked': 0,
    'interval_start': None,
    'interval_end': None
}

BOT_COMMANDS = [
    BotCommand(command='run', description='🟢 Запуск таймера'),
    BotCommand(command='smoke', description='🚬 Выкурить сигарету'),
    BotCommand(command='stop', description='🔴 Останов таймера'),
    BotCommand(command='status', description='ℹ️ Текущий статус'),
    BotCommand(command='settings', description='⚙️ Настройки'),
    BotCommand(command='help', description='🆘 Помощь'),
    # BotCommand(command='start', description='Начать работу с ботом'),
]

SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 3600

BOT_STARTED_MSG = 'Бот запущен ☑\nВсего пользователей *{}*'

HELP_MSG = (
    'Для управления используй кнопку _Меню_ внизу экрана\n\n'
    '__Команды бота:__\n\n'
    '/run \\- 🟢 запустить таймер\n'
    '/smoke \\- 🚬 выкурить сигарету\n'
    '/stop \\- 🔴 остановить таймер\n\n'
    '/status \\- ℹ️ текущий статус бота\n\n'
    '/settings \\- ⚙️ показать настройки\n\n'
    'Установка настроек \\(только с клавиатуры\\):\n'
    '⌨️ `/setinterval 60` \\- _установить интервал в минутах между '
    'сигаретами_\n'
    '⌨️ `/setinitial 1` \\- _установить начальное кол\\-во доступных '
    'сигарет после запуска таймера_\n'
    '⌨️ `/settz 3` \\- _установить местное время UTC\\+3_\n\n'
    '/help \\- 🆘 показать это сообщение\n'
)

START_MSG = (
    'Ну здравствуй, {}\\! 😐\n\n'
    'Прочти /help и установи себе настройки ⚙️ /settings\n\n'
    'Для управления используй кнопку _Меню_ внизу экрана\n\n'
    '\\.\\.\\. и помни \\- я слежу за тобой 🤫'
)

SETTINGS_MSG = (
    '⚙️ __*Текущие настройки*:__\n'
    'Интревал между 🚬: {} мин\n'
    'Сигарет в начале: {}\n'
    'Часовой пояс: UTC\\{:+}\n\n'
)

SETTING_SUCCESS_PREFIX = '🆗 Настройка успешно изменена\\.\n\n'

SETTING_ERROR_PREFIX = '⚠️ *Настройка не изменена\\!*\n'

SETINTERVAL_ERROR_MSG = (
    SETTING_ERROR_PREFIX +
    'Введите целое положительное число'
)

SETINITIAL_ERROR_MSG = (
    SETTING_ERROR_PREFIX +
    'Введите целое неотрицательное число'
)

SETTZ_ERROR_MSG = (
    SETTING_ERROR_PREFIX +
    'Введите целое число от \\-12 до \\+12'
)

STATUS_PREFIX = 'ℹ️'

STATUS_RUNNING_MSG = (
    STATUS_PREFIX
    + (
        'Таймер *запущен* 🟢\n\n'
        'Доступно сигарет:  *{}*\n'
        'Следующая 🚬 в {}\n'
        'Осталось ⏱️  *{}*\n\n'
        '_Обновлено {}_\n'
    )
)

STATUS_STOPPED_MSG = STATUS_PREFIX + 'Таймер _остановлен_ 🔴\n'

RUN_ALREADY_RUNNING_MSG = 'Таймер *уже* запущен 🟢'

ERROR_CHECK_SETTINGS_MSG = (
    '⚠️ _Внутренняя ошибка\nПроверьте настройки\\!_\n\n'
)

NEW_CIG_AVALABLE_MSG = (
    'Доступна *\\+1* 🚬 \\!\n'
    'Всего доступно: *{}*\n'
)

STOP_SMOKED_CIGS_MSG = 'Всего выкурено *{}* сигарет'

SMOKE_AFFIRMATIVE_MSG = '🚬 выкурена 🆗\n'

SMOKE_NEGATIVE_PREFIX = 'Отставить\\! 🤬\n'

SMOKE_NEGATIVE_NO_SIG_AVAILABLE = (
    SMOKE_NEGATIVE_PREFIX
    + 'Время не подошло, доступных 🚬 нет\\!'
)

SMOKE_NEGATIVE_STOPPED_TIMER = (
    SMOKE_NEGATIVE_PREFIX
    + 'Сначала запусти таймер ⏱️'
)

DEFAULT_REPLY_MSG = (
    '~{}~ \\- со своей болтовнёй иди к ChatGPT\\.\n'
    'Я занят, не мешай\\! 🤬🤬🤬'
)
