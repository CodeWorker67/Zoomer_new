from typing import List

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def create_kb(width: int,
              *args: str,
              **kwargs: str) -> InlineKeyboardMarkup:
    """
    Создает инлайн-клавиатуру на лету с заданными параметрами.
    """
    # Инициализируем билдер для создания инлайн-клавиатуры
    kb_builder = InlineKeyboardBuilder()
    # Список для хранения созданных кнопок
    buttons: List[InlineKeyboardButton] = []

    # В текущей реализации args не используется, оставлено для будущего расширения
    if args:
        # Здесь может быть добавлена обработка позиционных аргументов
        pass

    # Обрабатываем именованные аргументы (callback_data: text)
    if kwargs:
        for button_data, button_text in kwargs.items():
            # Создаем кнопку с текстом и callback-данными
            buttons.append(InlineKeyboardButton(
                text=button_text,
                callback_data=button_data
            ))

    # Распаковываем список кнопок в билдер, формируя ряды по width кнопок
    kb_builder.row(*buttons, width=width)

    # Возвращаем собранную клавиатуру
    return kb_builder.as_markup()


def chanel_keyboard():
    # Создаем клавиатуру с инлайн-кнопками
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Подписаться на канал",
                url="https://t.me/+C3B1C6zruYc4M2Ey"
            )
        ]
    ])
    return keyboard


def keyboard_start_bonus():
    keyboard = create_kb(1,
                         free_vpn='🔥 Попробовать бесплатно')
    return keyboard


def keyboard_start():
    keyboard = create_kb(1,
                         buy_vpn='🛒 Купить подписку',
                         connect_vpn='🔗 Подключить VPN',
                         ref='👥 Рефералка',
                         buy_gift='🎁 Подарить подписку',
                         info='💡 Информация')
    return keyboard


def keyboard_tariff_bonus():
    return create_kb(1,
                     r_7='🤌 7 дней - 99 руб',
                     r_30='🤝 30 дней - 139 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный интернет - 299 руб',
                     free_vpn='🔥ПОПРОБОВАТЬ 5 дней БЕСПЛАТНО🔥',
                     back_to_main='🔙 Назад'
                     )


def keyboard_tariff():
    return create_kb(1,
                     r_7='🤌 7 дней - 99 руб',
                     r_30='🤝 30 дней - 139 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный интернет - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_tariff_trial():
    return create_kb(1,
                     r_7='🤌 7 дней - 99 руб',
                     r_30='🤝 30 дней - 139 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_120='🔥 Акция: 120 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный интернет - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_tariff_old():
    return create_kb(1,
                     r_30old='🤝 30 дней - 99 руб',
                     r_90='👌 90 дней - 269 руб',
                     r_180='💪 180 дней - 499 руб',
                     r_white_30='🦾 Включи мобильный интернет - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_gift_tariff():
    return create_kb(1,
                     gift_r_7='🤌 7 дней - 99 руб',
                     gift_r_30='🤝 30 дней - 139 руб',
                     gift_r_90='👌 90 дней - 269 руб',
                     gift_r_180='💪 180 дней - 499 руб',
                     gift_r_white_30='🦾 Включи мобильный интернет - 299 руб',
                     back_to_main='🔙 Назад'
                     )


def keyboard_subscription(sub_url, sub_url_white):
    buttons = []
    if sub_url:
        buttons.append([InlineKeyboardButton(text="💫 Ваша подписка на VPN PRO", url=sub_url)])
    if sub_url_white:
        buttons.append([InlineKeyboardButton(text="🦾 Включи мобильный интернет", url=sub_url_white)])
    buttons.append([InlineKeyboardButton(text="❌ Если страница не загружается", callback_data='import')])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_sub_after_buy(sub_url):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 В личный кабинет", url=sub_url)],
        [InlineKeyboardButton(text="❌ Если страница не загружается", callback_data='import')],
        [InlineKeyboardButton(text="🎁 Подарить подписку", callback_data="buy_gift")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_import_os():
    return create_kb(1,
                     import_android='🤖 Android',
                     import_ios='🍎 iOS',
                     import_windows='🖥️ Windows',
                     import_macos='🍏 MacOS',
                     back_to_main='🔙 Назад')


def keyboard_import_app(os_callback: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Happ", callback_data=f"{os_callback}_happ")],
        [InlineKeyboardButton(text="📡 V2raytun", callback_data=f"{os_callback}_v2")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
    ])


def keyboard_import_sub(app_callback: str, has_casual: bool, has_white: bool):
    buttons = []
    if has_casual:
        buttons.append([InlineKeyboardButton(text="💫 Ваша подписка на VPN PRO", callback_data=f"{app_callback}_casual")])
    if has_white:
        buttons.append([InlineKeyboardButton(text="🦾 Включи мобильный интернет", callback_data=f"{app_callback}_white")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_import_end(url_app: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать приложение", url=url_app)],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
    ])


def keyboard_payment_cancel():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Купить подписку", callback_data="buy_vpn")],
        [InlineKeyboardButton(text="🎁 Подарить подписку", callback_data="start_gift")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_payment_method(tarif):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ СБП", callback_data=f"sbp_{tarif}")],
        [InlineKeyboardButton(text="💳 Карта РФ", callback_data=f"card_{tarif}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"stars_{tarif}")],
        [InlineKeyboardButton(text="💎 Crypto bot", callback_data=f"crypto_{tarif}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_payment_method_stock(tarif):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ СБП", callback_data=f"sbp_{tarif}")],
        [InlineKeyboardButton(text="💳 Карта РФ", callback_data=f"card_{tarif}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"stars_{tarif}")],
        [InlineKeyboardButton(text="💎 Crypto bot", callback_data=f"crypto_{tarif}")],
    ])
    return keyboard


def keyboard_payment_sbp(text, pay_url):
    return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=text, url=pay_url)]
            ])


def keyboard_payment_stars(stars_amount):
    return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Оплатить {stars_amount} ⭐️", pay=True)]
            ])


def ref_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Пригласить друзей🫶",
                switch_inline_query="partner"
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])
    return keyboard


def keyboard_inline_ref(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔗 Подключить VPN",
            url=f"https://t.me/zoomerskyvpn_bot?start=ref{user_id}")]
    ]
    )
