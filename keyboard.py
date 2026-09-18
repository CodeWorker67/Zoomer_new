import urllib.parse
from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_URL, PUBLIC_SITE_URL
from aiogram.utils.keyboard import InlineKeyboardBuilder

STYLE_PRIMARY = "primary"
STYLE_SUCCESS = "success"
STYLE_DANGER = "danger"

BTN_BACK = "◀️ Назад"

SITE_URL = "https://4zoomer.top/"
OPEN_SITE_CB = "open_site"


def create_kb(
    width: int,
    *,
    styles: Optional[dict[str, str]] = None,
    icons: Optional[dict[str, str]] = None,
    **kwargs: str,
) -> InlineKeyboardMarkup:
    """
    Создает инлайн-клавиатуру. kwargs: callback_data -> текст кнопки.
    styles: callback_data -> 'primary' | 'success' | 'danger' (цвет кнопки в клиентах Telegram).
    icons: callback_data -> icon_custom_emoji_id (кастомный эмодзи перед текстом кнопки).
    """
    kb_builder = InlineKeyboardBuilder()
    buttons: List[InlineKeyboardButton] = []
    style_map = styles or {}
    icon_map = icons or {}

    for button_data, button_text in kwargs.items():
        btn_kwargs: dict = {
            'text': button_text,
            'callback_data': button_data,
        }
        if icon_map.get(button_data):
            btn_kwargs['icon_custom_emoji_id'] = icon_map[button_data]
        if style_map.get(button_data):
            btn_kwargs['style'] = style_map[button_data]
        buttons.append(InlineKeyboardButton(**btn_kwargs))

    kb_builder.row(*buttons, width=width)
    return kb_builder.as_markup()


def chanel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Подписаться на канал",
                url="https://t.me/+C3B1C6zruYc4M2Ey",
            )
        ]
    ])
    return keyboard


ABOUT_SERVICE_CB = "about_service"


RAFFLE_CB = "raffle"
RAFFLE_PARTICIPATE_CB = "raffle_participate"
RAFFLE_TOP_CB = "raffle_top"
RAFFLE_BACK_CB = "raffle_back"
RAFFLE_RULES_URL = "https://telegra.ph/Usloviya-rozygrysha-Zumerskij-darit-09-17"
RAFFLE_BUTTON_EMOJI_ID = "6071303599973995501"


def keyboard_start(
    *,
    has_active_sub: bool = False,
    buy_primary: bool = True,
    sub_url: Optional[str] = None,
    show_trial: bool = False,
    show_raffle: bool = True,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if has_active_sub:
        if sub_url:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="🔗 Подключить VPN",
                        url=sub_url,
                        style=STYLE_PRIMARY,
                    )
                ]
            )
        rows.append(
            [
                InlineKeyboardButton(
                    text="Управление подпиской",
                    callback_data="connect_vpn",
                )
            ]
        )
    buy_kwargs = {"text": "💰 Купить подписку", "callback_data": "buy_vpn"}
    if buy_primary:
        buy_kwargs["style"] = STYLE_PRIMARY
    rows.append([InlineKeyboardButton(**buy_kwargs)])
    if show_raffle:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Розыгрыш",
                    callback_data=RAFFLE_CB,
                    icon_custom_emoji_id=RAFFLE_BUTTON_EMOJI_ID,
                )
            ]
        )
    if show_trial:
        rows.append(
            [InlineKeyboardButton(text="Попробовать бесплатно", callback_data="free_vpn")]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="💸 Заработок",
                callback_data="earn_with_us",
            ),
            InlineKeyboardButton(
                text="🌐 Наш сайт",
                callback_data=OPEN_SITE_CB,
            ),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="О сервисе", callback_data=ABOUT_SERVICE_CB),
            InlineKeyboardButton(
                text="Поддержка",
                url="https://t.me/Helpzoomerbot",
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_trial_existing_expired() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )


def keyboard_subscription_manage(sub_url: str) -> InlineKeyboardMarkup:
    from wl_traffic.constants import WL_TRAFFIC_BUY_CB

    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="📦 Купить трафик",
                callback_data=WL_TRAFFIC_BUY_CB,
            )
        ],
        [
            InlineKeyboardButton(
                text="Управление устройствами",
                callback_data="manage_devices",
            ),
        ],
        [
            InlineKeyboardButton(
                text="Если страница не загружается",
                callback_data="import",
            )
        ],
        [InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_about_service() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Пользовательское соглашение",
                    url="https://telegra.ph/Polzovatelskoe-soglashenie-08-11-20",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Политика конфиденциальности",
                    url="https://telegra.ph/Politika-konfidencialnosti-08-11-52",
                )
            ],
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )


def keyboard_buy_menu() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        buy_vpn_self='👤 Для себя',
        buy_gift='🎁 Подарить подписку',
        back_to_main=BTN_BACK,
    )


def keyboard_raffle(*, show_back_to_desc: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="Принять участие", callback_data=RAFFLE_PARTICIPATE_CB)],
        [InlineKeyboardButton(text="Условия участия в конкурсе", url=RAFFLE_RULES_URL)],
    ]
    if show_back_to_desc:
        rows.append(
            [InlineKeyboardButton(text="К описанию розыгрыша", callback_data=RAFFLE_BACK_CB)]
        )
    else:
        rows.append(
            [InlineKeyboardButton(text="Топ держателей билетов", callback_data=RAFFLE_TOP_CB)]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_earn_with_us() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        ref='👭 Бесплатный VPN за приглашения',
        partner_earn='🔗 Партнерская ссылка',
        create_partner_bot='🤖 Хочу своего бота',
        back_to_main=BTN_BACK,
    )


_TARIFF_LABELS = {
    '7': '7 дней — 99 руб',
    '30': '30 дней — 299 руб',
    '90': '90 дней — 749 руб (выгода −17%)',
    '180': '180 дней — 1349 руб (выгода −25%)',
    '365': '365 дней — 2399 руб (выгода −33%)',
    '730': '2 года — 3699 руб (выгода −50%)',
}

_USER_TARIFF_EMOJI = {
    '7': '👌 ',
    '30': '🤝 ',
    '90': '✅ ',
    '180': '🏆 ',
    '365': '💎 ',
    '730': '🔥 ',
}

_ADMIN_TARIFF_TICKETS = {
    '30': 1,
    '90': 3,
    '180': 6,
    '365': 12,
    '730': 24,
}


def ticket_prefix(key: str) -> str:
    n = _ADMIN_TARIFF_TICKETS.get(key)
    if not n:
        return ''
    return f'{n}🎟 '


def tariff_button_label(key: str, *, is_admin: bool = False) -> str:
    prefix = ticket_prefix(key)
    if prefix:
        return f'{prefix}{_TARIFF_LABELS[key]}'
    return f'{_USER_TARIFF_EMOJI[key]}{_TARIFF_LABELS[key]}'


def trial_discount_tariff_button_text(key: str, *, is_admin: bool = False) -> str:
    import re
    from lexicon import trial_discounted_rub

    if key not in _TARIFF_LABELS:
        return tariff_button_label(key, is_admin=is_admin)
    price = trial_discounted_rub(key)
    label = re.sub(
        r' — \d+ руб.*',
        f' — {price} руб (−20%)',
        _TARIFF_LABELS[key],
    )
    prefix = ticket_prefix(key)
    if prefix:
        return f'{prefix}{label}'
    return f'{_USER_TARIFF_EMOJI[key]}{label}'


def _tariff_button_kwargs(*, is_admin: bool = False, trial_discount: bool = False) -> dict[str, str]:
    if trial_discount:
        return {
            f'r_{key}': trial_discount_tariff_button_text(key, is_admin=is_admin)
            for key in _TARIFF_LABELS
        }
    return {
        f'r_{key}': tariff_button_label(key, is_admin=is_admin)
        for key in _TARIFF_LABELS
    }


def _tariff_kb(*, is_admin: bool = False, trial_discount: bool = False, **extra: str) -> InlineKeyboardMarkup:
    return create_kb(
        1,
        **_tariff_button_kwargs(is_admin=is_admin, trial_discount=trial_discount),
        **extra,
    )


def keyboard_tariff_bonus(*, is_admin: bool = False, trial_discount: bool = False):
    return _tariff_kb(
        is_admin=is_admin,
        trial_discount=trial_discount,
        free_vpn='🔥ПОПРОБОВАТЬ 1 день БЕСПЛАТНО🔥',
        wl_traffic_buy_sub='📦 Купить трафик Антиглушилка',
        back_to_buy_menu='🔙 Назад',
    )


def keyboard_tariff(*, is_admin: bool = False, trial_discount: bool = False):
    return _tariff_kb(
        is_admin=is_admin,
        trial_discount=trial_discount,
        wl_traffic_buy_sub='📦 Купить трафик Антиглушилка',
        back_to_buy_menu='🔙 Назад',
    )


def keyboard_tariff_trial(*, is_admin: bool = False, trial_discount: bool = False):
    return _tariff_kb(
        is_admin=is_admin,
        trial_discount=trial_discount,
        wl_traffic_buy_sub='📦 Купить трафик Антиглушилка',
        back_to_buy_menu='🔙 Назад',
    )


def keyboard_tariff_old():
    return create_kb(
        1,
        r_30old='🤝 30 дней — 99 руб',
        r_90='✅ 90 дней — 749 руб (выгода −17%)',
        r_180='🏆 180 дней — 1349 руб (выгода −25%)',
        r_365='💎 365 дней — 2399 руб (выгода −33%)',
        r_730='🔥 2 года — 3699 руб (выгода −50%)',
        back_to_main='🔙 Назад',
    )


def keyboard_gift_tariff(*, is_admin: bool = False):
    return create_kb(
        1,
        **{
            f'gift_r_{key}': tariff_button_label(key, is_admin=is_admin)
            for key in _TARIFF_LABELS
        },
        back_to_buy_menu='🔙 Назад',
    )


def keyboard_gift_tariff_repeat(*, is_admin: bool = False):
    from lexicon import format_gift_tariff_label

    return create_kb(
        1,
        **{
            f'gift_r_{key}': format_gift_tariff_label(
                key, repeat_giver=True, is_admin=is_admin
            )
            for key in _TARIFF_LABELS
        },
        back_to_buy_menu='🔙 Назад',
    )


def keyboard_subscription(sub_url):
    buttons = []
    if sub_url:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="💫 Ваша подписка на VPN PRO",
                    url=sub_url,
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(
                text="🌐 Войти через сайт",
                callback_data=OPEN_SITE_CB,
            )
        ]
    )
    buttons.append(
        [
            InlineKeyboardButton(
                text="⚠️ Если страница не загружается",
                callback_data='import',
            )
        ]
    )
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_sub_after_buy(sub_url):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📋 В личный кабинет",
                url=sub_url,
            )
        ],
        [
            InlineKeyboardButton(
                text="🌐 Войти через сайт",
                callback_data=OPEN_SITE_CB,
            )
        ],
        [
            InlineKeyboardButton(
                text="⚠️ Если страница не загружается",
                callback_data='import',
            )
        ],
        [
            InlineKeyboardButton(
                text="🎁 Подарить подписку",
                callback_data="buy_gift",
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_sub_after_free(sub_url):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📋 В личный кабинет",
                url=sub_url,
            )
        ],
        [
            InlineKeyboardButton(
                text="🌐 Войти через сайт",
                callback_data=OPEN_SITE_CB,
            )
        ],
        [
            InlineKeyboardButton(
                text="⚠️ Если страница не загружается",
                callback_data="import",
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
    ])
    return keyboard


def keyboard_import_os():
    return create_kb(
        1,
        import_android='🤖 Android',
        import_ios='🍎 iOS',
        import_windows='🖥️ Windows',
        import_macos='🍏 MacOS',
        back_to_main='🔙 Назад',
    )


def keyboard_import_app(os_callback: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🔥 INCY",
                callback_data=f"{os_callback}_incy",
            )
        ],
        [
            InlineKeyboardButton(
                text="⭐️ Happ",
                callback_data=f"{os_callback}_happ",
            )
        ],
        [
            InlineKeyboardButton(
                text="📡 V2raytun",
                callback_data=f"{os_callback}_v2",
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="import")],
    ])


def keyboard_import_after_album() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        connect_vpn="🔙 Назад к подписке",
    )


def keyboard_import_sub(app_callback: str, has_casual: bool):
    buttons = []
    if has_casual:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="💫 Ваша подписка на VPN PRO",
                    callback_data=f"{app_callback}_casual",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_import_end(url_app: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📥 Скачать приложение",
                url=url_app,
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
    ])


def keyboard_payment_cancel():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="💰 Купить подписку",
                callback_data="buy_vpn",
            )
        ],
        [
            InlineKeyboardButton(
                text="🎁 Подарить подписку",
                callback_data="start_gift",
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data='back_to_main')],
    ])
    return keyboard


def keyboard_wheel_discount(kind: str, product_key: str, counts) -> InlineKeyboardMarkup:
    """kind: sub | gift | traffic"""
    rows = []
    for pct, n in ((10, counts.discount_10), (30, counts.discount_30), (50, counts.discount_50)):
        if n > 0:
            rows.append([
                InlineKeyboardButton(
                    text=f"🎡 −{pct}% (осталось {n})",
                    callback_data=f"wd_p:{kind}:{product_key}:{pct}",
                )
            ])
    rows.append([
        InlineKeyboardButton(
            text="Без скидки",
            callback_data=f"wd_p:{kind}:{product_key}:0",
        )
    ])
    if kind == "traffic":
        back_cb = "wl_traffic_buy"
    elif kind == "gift":
        back_cb = "buy_gift"
    else:
        back_cb = "buy_vpn"
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_payment_method(tarif, *, hide_back: bool = False):
    rows = [
        [InlineKeyboardButton(text="⚡СБП", callback_data=f"wata_sbp_{tarif}")],
        [InlineKeyboardButton(text="💳 Карта РФ", callback_data=f"wata_card_{tarif}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"stars_{tarif}")],
        [InlineKeyboardButton(text="💎 Crypto bot", callback_data=f"crypto_{tarif}")],
    ]
    if not hide_back:
        rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_payment_method_stock(tarif):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="⚡СБП",
                callback_data=f"wata_sbp_{tarif}",
            )
        ],
        [
            InlineKeyboardButton(
                text="💳 Карта РФ",
                callback_data=f"wata_card_{tarif}",
            )
        ],
        [
            InlineKeyboardButton(
                text="⭐️ Telegram Stars",
                callback_data=f"stars_{tarif}",
            )
        ],
        [
            InlineKeyboardButton(
                text="💎 Crypto bot",
                callback_data=f"crypto_{tarif}",
            )
        ],
    ])
    return keyboard


def keyboard_payment_sbp(text, pay_url):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=text,
                url=pay_url,
            )
        ]
    ])


def keyboard_payment_stars(stars_amount):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"Оплатить {stars_amount} ⭐️",
                pay=True,
            )
        ]
    ])


def _site_base_url() -> str:
    return (PUBLIC_SITE_URL or SITE_URL or "").strip().rstrip("/")


def partner_bot_link(user_id: int) -> str:
    base = (BOT_URL or "").rstrip("/")
    return f"{base}?start=partner_{user_id}"


def partner_site_link(user_id: int) -> Optional[str]:
    site = _site_base_url()
    if not site:
        return None
    return f"{site}?start=partner_{user_id}"


def partner_invite_share_text(user_id: int) -> str:
    lines = [
        "💸 Зумерский VPN — подключайся по моей партнёрской ссылке "
        "к быстрому и надёжному VPN!",
        "",
        f"🤖 Бот: {partner_bot_link(user_id)}",
    ]
    site = partner_site_link(user_id)
    if site:
        lines.append(f"🌐 Сайт: {site}")
    return "\n".join(lines)


def partner_invite_share_url(user_id: int) -> str:
    bot_link = partner_bot_link(user_id)
    inner = urllib.parse.quote(bot_link, safe="")
    text = urllib.parse.quote(partner_invite_share_text(user_id))
    return f"https://t.me/share/url?url={inner}&text={text}"


def ref_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Пригласить друзей🫶",
                    url=f"https://t.me/share/url?url={BOT_URL}?start=ref{user_id}&text={urllib.parse.quote('Вот ссылка для тебя на надёжный VPN!')}",
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_earn")],
        ]
    )
    return keyboard


def keyboard_inline_ref(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🔗 Подключить VPN",
                url=f"{BOT_URL}?start=ref{user_id}",
            )
        ]
    ])


def keyboard_inline_partner(user_id: int):
    rows = [
        [
            InlineKeyboardButton(
                text="🔗 Подключить VPN",
                url=partner_bot_link(user_id),
            )
        ]
    ]
    site = partner_site_link(user_id)
    if site:
        rows.append([
            InlineKeyboardButton(
                text="🌐 Открыть сайт",
                url=site,
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_partner_dashboard(user_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Пригласить друзей 🌠",
                    url=partner_invite_share_url(user_id),
                )
            ],
            [
                InlineKeyboardButton(
                    text="💰 Создать заявку на вывод",
                    callback_data="partner_withdraw",
                )
            ],
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_earn")],
        ]
    )


def keyboard_devices_subscriptions(slots: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    """slots: (ключ слота, текст кнопки)."""
    buttons = []
    for slot_key, label in slots:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=label[:64],
                    callback_data=f"dev_sub_{slot_key}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_devices_list(
    slot_key: str,
    devices: list[tuple[int, str]],
) -> InlineKeyboardMarkup:
    """devices: (индекс, текст кнопки)."""
    buttons = []
    for idx, btn_text in devices:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=btn_text[:64],
                    callback_data=f"dev_rm_{slot_key}_{idx}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text=BTN_BACK, callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_devices_confirm(slot_key: str, device_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Да",
                    callback_data=f"dev_rm_yes_{slot_key}_{device_idx}",
                ),
                InlineKeyboardButton(
                    text="❌ Нет",
                    callback_data=f"dev_sub_{slot_key}",
                ),
            ],
        ]
    )


def keyboard_partner_withdraw(support_url: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="💬 Вывести деньги",
                url=support_url,
            )
        ],
        [
            InlineKeyboardButton(
                text="🔙 Назад",
                callback_data="partner_earn",
            )
        ],
    ])


def keyboard_profile() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        manage_devices="📱 Управление устройствами",
        wl_traffic_buy="📦 Купить трафик",
        back_to_main=BTN_BACK,
    )


def keyboard_wl_traffic_tariffs(*, back_callback: str = "back_to_main") -> InlineKeyboardMarkup:
    from wl_traffic.constants import WL_TRAFFIC_TARIFFS
    from_sub = back_callback in ("buy_vpn", "buy_vpn_self")
    buttons = []
    for gb, price in sorted(WL_TRAFFIC_TARIFFS.items(), key=lambda item: int(item[0]), reverse=True):
        cb = f"wl_traffic_sub_{gb}" if from_sub else f"wl_traffic_{gb}"
        buttons.append([
            InlineKeyboardButton(
                text=f"{gb} GB — {price} ₽",
                callback_data=cb,
            )
        ])
    buttons.append([InlineKeyboardButton(text=BTN_BACK, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_wl_traffic_payment_method(
    mb: str,
    *,
    back_callback: str = "wl_traffic_buy",
    hide_back: bool = False,
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="⚡СБП", callback_data=f"wl_traffic_sbp_{mb}")],
        [InlineKeyboardButton(text="💳 Карта РФ", callback_data=f"wl_traffic_card_{mb}")],
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data=f"wl_traffic_stars_{mb}")],
        [InlineKeyboardButton(text="💎 Crypto bot", callback_data=f"wl_traffic_crypto_{mb}")],
    ]
    if not hide_back:
        rows.append([InlineKeyboardButton(text=BTN_BACK, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)
