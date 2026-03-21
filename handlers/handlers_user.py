from bot import sql, x3, bot
from config import CHANEL_ID
from keyboard import (keyboard_start, keyboard_start_bonus, keyboard_tariff_bonus, keyboard_tariff,
                      keyboard_subscription, ref_keyboard, keyboard_gift_tariff,
                      keyboard_payment_method, keyboard_payment_method_stock, chanel_keyboard, create_kb,
                      keyboard_inline_ref)
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, InlineQueryResultArticle, InputTextMessageContent, \
    InlineQuery
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import lexicon


router: Router = Router()


# Этот хэндлер срабатывает на команду /start
@router.message(Command(commands="start"))
async def process_start_command(message: Message, command: Command):

    user_data = await sql.SELECT_ID(message.from_user.id)
    has_paid_subscription = False
    in_chanel = False
    ref_login = ''
    existing = False
    stamp = ''
    ttclid = None

    if user_data:
        has_paid_subscription = user_data[4]
        in_chanel = user_data[7]
        existing = True

    if len(message.text.split(' ')) == 1:
        if user_data:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно')
        else:
            logger.success(f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз')

    else:
        if 'ref' in message.text:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с реферальной ссылкой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по реферальной ссылкой')
                ref_login = message.text.split(' ')[1].replace('ref', '')

        elif 'gift_' in message.text:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} пытается активировать подарочную подписку')
            gift_id = message.text.split(' ')[1].replace('gift_', '')
            has_paid_subscription = await activate_gift(message, gift_id)
            await asyncio.sleep(2)
            existing = True
        else:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке')
                stamp = message.text.split(' ')[1]

    if not existing:
        await sql.INSERT(message.from_user.id, False, False, ref=ref_login, stamp=stamp)
        logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} добавлен в БД')
        if ttclid:
            await sql.UPDATE_TTCLID(message.from_user.id, ttclid)
            logger.info(f'Юзеру {message.from_user.id} - {message.from_user.username} присвоен ttclid')

    if not has_paid_subscription:
        await message.answer(text=lexicon['start_bonus'],
                             reply_markup=keyboard_start_bonus(),
                             disable_web_page_preview=True)
    else:
        await message.answer(text=lexicon['start'],
                             reply_markup=keyboard_start(),
                             disable_web_page_preview=True)


@router.callback_query(F.data == 'buy_vpn')
async def buy_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    user_data = await sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False

    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]

    result_active = await x3.activ(str(callback.from_user.id))

    if result_active['activ'] == '🔎 - Не подключён' and not has_paid_subscription:
        await callback.message.answer(text=lexicon['buy'],
                                      reply_markup=keyboard_tariff_bonus(),
                                      disable_web_page_preview=True)
    else:
        await callback.message.answer(text=lexicon['buy'],
                                      reply_markup=keyboard_tariff(),
                                      disable_web_page_preview=True)


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    # await x3.test_connect()
    user_id = str(callback.from_user.id)
    sub_url = await x3.sublink(user_id)
    sub_url_white = None
    user_data = await sql.SELECT_ID(callback.from_user.id)
    if user_data[10]:
        user_id_white = user_id + '_white'
        sub_url_white = await x3.sublink(user_id_white)

    if not sub_url and not sub_url_white:
        await callback.message.answer(lexicon['no_sub'])
        return

    await callback.message.answer(
        text=lexicon['to_sub'],
        reply_markup=keyboard_subscription(sub_url, sub_url_white),
        disable_web_page_preview=True
    )
    await callback.answer()


@router.callback_query(F.data.in_({'r_7', 'r_30', 'r_90', 'r_180', 'r_white_30', 'r_30old'}))
async def process_payment_method(callback: CallbackQuery):
    await callback.answer()
    text = lexicon['payment_link']
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    text += '\n\nВыберите способ оплаты:'
    tariff = callback.data
    await callback.message.answer(text, reply_markup=keyboard_payment_method(tariff))


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_cb(callback: CallbackQuery):
    day = 5

    user_data = await sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False
    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]
    if has_paid_subscription:
        await callback.message.answer(text=lexicon['free_vpn_no'],
                                      reply_markup=keyboard_start())
        return
    # Проверка на наличие данных
    # await x3.test_connect()
    logger.info(await x3.addClient(day, str(callback.from_user.id), int(callback.from_user.id)))
    result_active = await x3.activ(str(callback.from_user.id))
    time = result_active['time']

    # Проверка на наличие данных
    if await sql.SELECT_ID(callback.from_user.id) is not None:
        await sql.UPDATE_PAYNULL(callback.from_user.id)
    else:
        await sql.INSERT(callback.from_user.id, True)
    user_id = str(callback.from_user.id)
    sub_url = await x3.sublink(user_id)

    await callback.message.answer(text=lexicon['buy_success'].format(time, sub_url),
                                  reply_markup=keyboard_subscription(sub_url, None),
                                  disable_web_page_preview=True)
    await asyncio.sleep(1)
    await callback.message.answer(lexicon['to_chanel'], reply_markup=chanel_keyboard())
    await callback.answer()


@router.callback_query(F.data == 'info')
async def faq(callback: CallbackQuery):
    await callback.answer()
    user_data = await sql.SELECT_ID(callback.from_user.id)
    has_paid_subscription = False
    if user_data is not None and len(user_data) > 4:
        has_paid_subscription = user_data[4]
    if has_paid_subscription:
        await callback.message.answer(
            text=lexicon['start'],
            reply_markup=keyboard_start(),
            disable_web_page_preview=True
        )
    else:
        await callback.message.answer(
            text=lexicon['start_bonus'],
            reply_markup=keyboard_start_bonus(),
            disable_web_page_preview=True
        )


@router.callback_query(F.data == 'ref')
async def referral_program(callback: CallbackQuery):
    await callback.answer()
    count = await sql.SELECT_COUNT_REF(int(callback.from_user.id))
    await callback.message.answer(
        text=lexicon['ref_info'].format(count, callback.from_user.id),
        reply_markup=ref_keyboard(),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == 'buy_gift')
async def gift_subscription_start(callback: CallbackQuery):
    await callback.answer()
    """Начало процесса подарка подписки"""
    await callback.message.answer(
        lexicon['gift_start'],
        reply_markup=keyboard_gift_tariff()
    )


@router.callback_query(F.data.startswith('gift_'))
async def process_gift_payment_method(callback: CallbackQuery):
    await callback.answer()
    text = lexicon['payment_link']
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    tariff = callback.data
    text += '\n\nВыберите способ оплаты <b>подарочной подписки</b>:'
    await callback.message.answer(text, reply_markup=keyboard_payment_method(tariff))


async def activate_gift(message: Message, gift_id: str):
    """Активация подарка по gift_id"""
    result = await sql.activate_gift(gift_id, message.from_user.id)

    if not result[0]:
        await message.answer(lexicon['gift_no'])
        logger.warning(f'Ссылка на подарок протухла')
        if await sql.SELECT_ID(message.from_user.id) is None:
            await sql.INSERT(message.from_user.id, False)
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по подарочной ссылке')
        return False

    duration = result[1]
    white_flag = result[2]

    # Активируем подписку для получателя
    # await x3.test_connect()
    user_id = message.from_user.id
    user_id_str = str(message.from_user.id)
    if white_flag:
        user_id_str += '_white'

    # Важно: `x3.addClient/updateClient` проставляет end_date в БД через UPDATE.
    # Для нового пользователя строка в `users` еще не создана, поэтому UPDATE ничего не меняет.
    # Сначала гарантируем наличие пользователя в БД.
    was_in_db = await sql.SELECT_ID(message.from_user.id) is not None
    if not was_in_db:
        await sql.INSERT(message.from_user.id, False)


    # Проверяем существует ли пользователь
    existing_user = await x3.get_user_by_username(user_id_str)

    if existing_user and 'response' in existing_user and existing_user['response']:
        response = await x3.updateClient(duration, user_id_str, user_id)
    else:
        response = await x3.addClient(duration, user_id_str, user_id)

    if response:
        # Получаем информацию о подписке
        result_active = await x3.activ(user_id_str)
        subscription_time = result_active.get('time', '-')

        # Обновляем базу данных
        await sql.UPDATE_PAYNULL(message.from_user.id)
        if was_in_db:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} получил в подарок подписку, уже был в БД')
        else:
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз и получил подарочную подписку')

        # Отправляем сообщение получателю
        await message.answer(lexicon['gift_yes'].format(duration, subscription_time))
        return True

    else:
        await message.answer("❌ Ошибка при активации подарка. Обратитесь в поддержку.")
        if await sql.SELECT_ID(message.from_user.id) is None:
            await sql.INSERT(message.from_user.id, False)
        return False


@router.callback_query(F.data == 'video_faq')
async def video_faq(callback: CallbackQuery):
    await callback.message.answer_video(video='BAACAgIAAxkBAAEBk_5pmqIm8a5-5ioQ3GziIJ4dBH9PugAC_ZgAAtS92EjbvWnuAla0dDoE',
                                        caption=lexicon['push_not_subscribed_3h'],
                                        reply_markup=create_kb(1, back_to_main='🔙 Назад'))


@router.callback_query(F.data == 'back_to_buy_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.answer(text=lexicon['buy'], reply_markup=keyboard_tariff())


@router.callback_query(F.data == 'back_to_main')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.answer(text=lexicon['start'],
                                  reply_markup=keyboard_start(),
                                  disable_web_page_preview=True)


@router.callback_query(F.data == 'back_to_gift_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.message.edit_text(text=lexicon['gift_start'], reply_markup=keyboard_gift_tariff())


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def user_blocked_bot(event: ChatMemberUpdated):
    await sql.UPDATE_DELETE(event.from_user.id, True)
    logger.warning(f'Юзер {event.from_user.id} заблокировал бота')


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: ChatMemberUpdated):
    await sql.UPDATE_DELETE(event.from_user.id, False)
    logger.success(f'Юзер {event.from_user.id} разблокировал бота')


@router.callback_query(F.data == 'r_120')
async def process_payment_method_bonus(callback: CallbackQuery):
    user_data = await sql.SELECT_ID(callback.from_user.id)
    if user_data[8]:
        await callback.message.answer('Акция действительна только при первой оплате.',
                                      reply_markup=create_kb(1, back_to_main='🔙 Назад'))
    tariff = callback.data
    await callback.message.answer('Выберите метод оплаты акционной подписки:', reply_markup=keyboard_payment_method_stock(tariff))


@router.chat_member()
async def handle_chat_member_update(update: ChatMemberUpdated):
    if str(update.chat.id) != str(CHANEL_ID):
        return
    user_id = update.new_chat_member.user.id
    user_dct = await sql.SELECT_ID(user_id)

    if not user_dct:
        logger.warning(f"User in chanel {user_id} not found in database")
        return

    if update.old_chat_member.status == "left" and update.new_chat_member.status == "member":
        await sql.UPDATE_ADMIN(user_id, True)
        logger.success(f"User {user_id} connect to chanel")
    elif update.old_chat_member.status != "left" and update.new_chat_member.status == "left":
        await sql.UPDATE_ADMIN(user_id, False)
        logger.warning(f"User {user_id} left chanel")


@router.inline_query(lambda query: query.query == 'partner')
async def inline_partner(inline_query: InlineQuery):
    user_id = inline_query.from_user.id

    text = f'''
Привет. Подключись к VPN по моей ссылке:

https://t.me/zoomerskyvpn_bot?start=ref{user_id}

🚀Работает быстро и стабильно.
    '''

    result = InlineQueryResultArticle(
        id="1",
        title='🤝🤝🤝 Приглашение',
        description="Друг, перешедший по этой кнопке станет Вашим рефералом.",
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode='HTML',
            disable_web_page_preview=False
        ),
        reply_markup=keyboard_inline_ref(user_id),
        thumb_url="https://img.freepik.com/premium-photo/glowing-blue-neon-wifi-signal-icon-dark-background_989822-6238.jpg?semt=ais_hybrid"  # опционально: иконка
    )

    # Отправляем результат обратно в Telegram
    await bot.answer_inline_query(
        inline_query.id,
        results=[result],
        cache_time=0
    )
