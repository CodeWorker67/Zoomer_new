import random
from datetime import datetime, timezone

from sqlalchemy import select

from bot import sql, x3, bot
from config import ADMIN_IDS
from config_bd.models import Users
from keyboard import create_kb
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from sheduler.check_connect import check_connect

router = Router()


@router.message(F.video, F.from_user.id.in_(ADMIN_IDS))
async def get_video(message: Message):
    await message.answer(message.video.file_id)


@router.message(F.photo, F.from_user.id.in_(ADMIN_IDS))
async def get_photo(message: Message):
    await message.answer(message.photo[-1].file_id)


@router.message(Command(commands=['user']))
async def user_info(message: Message):

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /user <telegram_id>\nНапример: /user 123456789")
            return

        user_id = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.SELECT_ID(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return
        text = []
        for i in range(len(user_data)):
            if isinstance(user_data[i], datetime):
                item = user_data[i].strftime('%Y-%m-%d %H:%M:%S')
                text.append(item)
            elif user_data[i] is None:
                text.append('None')
            else:
                text.append(str(user_data[i]))
        text = '\n'.join(text)
        await message.answer(text)
    except Exception as e:
        await message.answer(f'Ошибка при формировании сообщения: {str(e)}')


@router.message(Command(commands=['sub']))
async def set_subscription_date(message: Message):
    """Установка subscription_end_date или white_subscription_end_date в БД и панели"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    try:
        args = message.text.split()
        if len(args) < 3:
            await message.answer(
                "❌ Использование:\n"
                "  /sub <telegram_id> <дата_время>               – обновить обычную подписку\n"
                "  /sub <telegram_id> white <дата_время>         – обновить белую подписку\n"
                "Примеры:\n"
                "  /sub 123456789 2026-02-01 17:14:27\n"
                "  /sub 123456789 white 2026-02-01 17:14:27\n"
                "Формат даты: YYYY-MM-DD HH:MM:SS"
            )
            return

        user_id = int(args[1].strip())

        # Определяем тип и позицию даты
        if args[2].lower() == 'white':
            is_white = True
            date_str = " ".join(args[3:])
        else:
            is_white = False
            date_str = " ".join(args[2:])

        # Парсим дату
        date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M:%S",
            "%d.%m.%Y %H:%M"
        ]
        target_date = None
        for fmt in date_formats:
            try:
                target_date = datetime.strptime(date_str, fmt)
                target_date = target_date.replace(tzinfo=timezone.utc)  # панель работает в UTC
                break
            except ValueError:
                continue
        if target_date is None:
            await message.answer(f"❌ Неверный формат даты: {date_str}")
            return

        # Проверяем наличие пользователя в БД
        user_data = await sql.SELECT_ID(user_id)
        if not user_data:
            await message.answer("⚠️ Пользователь не найден в БД.")
            return

        # Формируем username для панели
        username = str(user_id) + ('_white' if is_white else '')

        # Устанавливаем дату в панели
        success, actual_date = await x3.set_expiration_date(username, target_date, user_id)

        if not success or actual_date is None:
            await message.answer("❌ Не удалось установить дату в панели. Подробности в логах.")
            return

        if is_white:
            await sql.update_white_subscription_end_date(user_id, actual_date)
        else:
            await sql.update_subscription_end_date(user_id, actual_date)

        # Сообщаем результат
        await message.answer(
            f"✅ Дата подписки успешно установлена!\n\n"
            f"👤 Пользователь: {user_id}\n"
            f"📅 Целевая дата (UTC): {target_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📅 Установленная в панели дата (UTC): {actual_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📝 Тип: {'white' if is_white else 'обычная'}\n"
            f"💾 База данных обновлена."
        )
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"✅ Вам обновлена дата подписки!\n\n"
                f"📅 Новая дата окончания подписки: {actual_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📝 Тариф: {'🦾 Включи мобильный интернет' if is_white else '💫 подписка на VPN'}\n",
                reply_markup=create_kb(1, back_to_main='🔙 Назад')
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")
            await message.answer(f"❌ Произошла ошибка при отправке сообщения пользователю {user_id}: {str(e)}")

    except Exception as e:
        logger.error(f"Ошибка в команде /sub: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")


@router.message(Command(commands=['delete']))
async def delete_user_command(message: Message):
    """Удаление пользователя из БД по Telegram ID"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /delete <telegram_id>\nНапример: /delete 123456789")
            return

        user_id_to_delete = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.SELECT_ID(user_id_to_delete)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id_to_delete} не найден в базе данных.")
            return

        # Получаем информацию о пользователе для уведомления
        user_info = {
            "user_id": user_data[1],  # User_id
            "ref": user_data[2],  # Ref
            "is_pay_null": user_data[4],  # Is_pay_null
            "is_admin": user_data[7] if len(user_data) > 7 else False  # Is_admin
        }

        # УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ ИЗ БД
        deletion_success = await sql.DELETE(user_id_to_delete)

        if deletion_success:
            # Логируем действие
            logger.info(f"Администратор {message.from_user.id} удалил пользователя {user_id_to_delete} из БД")

            # Формируем отчет об удалении
            report_message = (
                f"✅ Пользователь успешно удалён из базы данных\n\n"
                f"📋 Информация об удалённом пользователе:\n"
                f"├ ID: {user_info['user_id']}\n"
                f"├ Реферер: {user_info['ref'] if user_info['ref'] else 'нет'}\n"
                f"├ Оплачивал: {'✅ да' if user_info['is_pay_null'] else '❌ нет'}\n"
                f"└ Администратор: {'✅ да' if user_info['is_admin'] else '❌ нет'}\n\n"
                f"⚠️ Пользователь удалён только из базы данных бота.\n"
                f"   Подписка в панели управления (X3) остаётся активной.\n"
                f"   Чтобы удалить полностью, используйте команду /gift на 0 дней."
            )

            await message.answer(report_message)

        else:
            await message.answer(f"❌ Ошибка при удалении пользователя {user_id_to_delete}.\n"
                                 "Возможно, пользователь уже был удалён или произошла ошибка базы данных.")

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID.\n"
                             "Используйте только цифры, например: /delete 123456789")
    except Exception as e:
        logger.error(f"Ошибка в команде /delete: {e}")
        await message.answer(f"❌ Произошла ошибка при выполнении команды: {str(e)}")


@router.message(Command("online"))
async def check_online(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()

    active_telegram_ids = []
    for user in users_x3:
        if user['userTraffic']['firstConnectedAt']:
            connected_str = user['userTraffic']['onlineAt']
            try:
                connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                connected_date = connected_dt.date()
                if connected_date == datetime.now().date():
                    telegram_id = user.get('telegramId')
                    if telegram_id is not None:
                        active_telegram_ids.append(int(telegram_id))
            except (ValueError, TypeError):
                continue

    count_pay = 0
    count_trial = 0
    for tg_id in active_telegram_ids:
        user_data = await sql.SELECT_ID(tg_id)
        if user_data:
            if user_data[8]:
                count_pay += 1
            else:
                count_trial += 1
    await message.answer(
        f"Всего юзеров в панели: {len(users_x3)}\n"
        f"Юзеров, которые были онлайн сегодня: {len(active_telegram_ids)}\n"
        f"Юзеры с платной подпиской: {count_pay}\n"
        f"Юзеры на триале: {count_trial}"
    )


@router.message(Command("balance_panel"))
async def check_online(message: Message):
    squad_1 = ['6ba41467-be68-438c-ad6e-5a02f7df826c']
    squad_2 = ['c6973051-58b7-484c-b669-6a123cda465b']
    squad_3 = ['a867561f-8736-4f67-8970-e20fddd00e5e']
    squad_4 = ['29b73cd8-8a68-41cd-99c7-5d30dbac4c71']
    squad_5 = ['d108d4a0-a121-4b52-baee-a97243208179']
    success_count = 0
    fail_count = 0
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()
    for user in users_x3:
        try:
            await asyncio.sleep(0.3)
            random_squad = random.choice([squad_1, squad_2, squad_3, squad_4, squad_5])
            username = user.get('username', '')
            if 'white' not in username and 'cascade-bridge-system' not in username:
                uuid = user.get('uuid')
                if user['userTraffic']['firstConnectedAt']:
                    connected_str = user['userTraffic']['onlineAt']
                    connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                    connected_date = connected_dt.date()
                    if connected_date == datetime.now().date() and uuid:
                        if await x3.update_user_squads(uuid, random_squad):
                            success_count += 1
                        else:
                            fail_count += 1
        except:
            pass
    await message.answer(f"{len(users_x3)} - всего юзеров в панели\n{success_count + fail_count} - онлайн сегодня\n{success_count} - обновлено\n{fail_count} - ошибка")


@router.message(Command(commands=['get_second']))
async def get_second_command(message: Message):
    """Проверяет, сколько пользователей с ttclid='second_chance_100326' были онлайн после 10.03.2026"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Получаю данные из панели и базы...")

    try:
        # 1. Получаем всех пользователей с нужным ttclid
        async with sql.session_factory() as session:
            stmt = select(Users.user_id).where(Users.ttclid == 'second_chance_100326')
            result = await session.execute(stmt)
            user_ids = [row[0] for row in result.all()]

        if not user_ids:
            await message.answer("❌ Нет пользователей с ttclid = second_chance_100326")
            return

        # 2. Загружаем всех пользователей из панели
        panel_users = await x3.get_all_users()  # список словарей с полными данными
        logger.info(f"Загружено {len(panel_users)} пользователей из панели")

        # 3. Строим множество telegram_id из панели для быстрого поиска
        #    и сохраняем дату последнего онлайна
        panel_dict = {}
        for user in panel_users:
            tg_id = user.get('telegramId')
            if tg_id is not None:
                panel_dict[int(tg_id)] = user

        # 4. Проверяем каждого пользователя из списка
        cutoff_date = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
        online_after_cutoff = 0
        not_found_in_panel = 0
        online_before_or_never = 0

        for uid in user_ids:
            user_panel = panel_dict.get(uid)
            if not user_panel:
                not_found_in_panel += 1
                continue

            # Проверяем onlineAt (последнее подключение)
            online_at_str = user_panel.get('userTraffic', {}).get('onlineAt')
            if not online_at_str:
                online_before_or_never += 1
                continue

            try:
                online_dt = datetime.fromisoformat(online_at_str.replace('Z', '+00:00'))
                if online_dt >= cutoff_date:
                    online_after_cutoff += 1
                else:
                    online_before_or_never += 1

            except (ValueError, TypeError):
                online_before_or_never += 1

        # 5. Формируем ответ
        report = (
            f"📊 Статистика по ttclid = second_chance_100326\n"
            f"👥 Всего в БД: {len(user_ids)}\n"
            f"✅ Онлайн после 10.03.2026: {online_after_cutoff}\n"
            f"❌ Не были онлайн после 10.03.2026 (или никогда): {online_before_or_never}\n"
            f"🔍 Не найдены в панели: {not_found_in_panel}"
        )
        await message.answer(report)
        logger.info(f"Админ {message.from_user.id} выполнил /get_second: {report}")

    except Exception as e:
        logger.error(f"Ошибка в /get_second: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(Command(commands=['check_users']))
async def check_users_command(message: Message):
    """Проверка соответствия дат окончания подписки у оплаченных пользователей (has_discount=True)"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Начинаю проверку пользователей с оплатами...")

    try:
        # 1. Получаем список оплаченных пользователей из БД
        users_with_discount = await sql.get_users_with_payment()
        total = len(users_with_discount)
        if total == 0:
            await message.answer("❌ Нет пользователей с оплатами.")
            return

        # 2. Получаем всех пользователей из панели (один запрос)
        panel_users = await x3.get_all_users()
        logger.info(f"Загружено {len(panel_users)} пользователей из панели")

        # 3. Строим словарь для быстрого поиска по telegramId и username
        panel_by_telegram = {}      # ключ: telegramId (int)
        panel_by_username = {}      # ключ: username (str)

        for user in panel_users:
            tg_id = user.get('telegramId')
            username = user.get('username')
            if tg_id is not None:
                panel_by_telegram[int(tg_id)] = user
            elif username:
                panel_by_username[username] = user

        # 4. Проходим по всем оплаченным пользователям и ищем их в панели
        mismatched = []      # кортежи (user_id, db_date, panel_date) для расхождений >=3ч
        not_found_in_panel = []  # пользователи, отсутствующие в панели
        processed = 0

        for user_id in users_with_discount:
            processed += 1
            if processed % 10 == 0:
                logger.info(f"Проверено {processed}/{total}")

            # Пытаемся найти пользователя в панели
            panel_user = panel_by_telegram.get(user_id)
            if panel_user is None:
                panel_user = panel_by_username.get(str(user_id))

            if panel_user is None:
                not_found_in_panel.append(user_id)
                continue

            expire_str = panel_user.get('expireAt')
            if not expire_str:
                # нет даты в панели – считаем расхождением (panel_date = None)
                db_expire = await sql.get_subscription_end_date(user_id)
                mismatched.append((user_id, db_expire, None))
                continue

            try:
                panel_expire = datetime.fromisoformat(expire_str.replace('Z', '+00:00'))
            except Exception:
                # не удалось распарсить дату панели
                db_expire = await sql.get_subscription_end_date(user_id)
                mismatched.append((user_id, db_expire, None))
                continue

            # Получаем дату из БД (обычная подписка)
            db_expire = await sql.get_subscription_end_date(user_id)
            panel_naive = panel_expire.replace(tzinfo=None)

            if db_expire is None:
                # нет даты в БД
                mismatched.append((user_id, None, panel_naive))
                continue

            db_naive = db_expire.replace(tzinfo=None)
            diff_hours = abs((panel_naive - db_naive).total_seconds()) / 3600

            if diff_hours >= 4:
                mismatched.append((user_id, db_naive, panel_naive))

        # 5. Формируем отчёт
        report_lines = []
        report_lines.append(f"📊 Результаты проверки:\n")
        report_lines.append(f"👥 Всего проверено: {total}")
        report_lines.append(f"❌ Расхождений в датах (>=4ч): {len(mismatched)}")
        report_lines.append(f"🔍 Не найдены в панели: {len(not_found_in_panel)}")

        # Если есть расхождения и их количество не превышает лимит для прямого вывода
        if mismatched or not_found_in_panel:
            if len(mismatched) <= 50 and len(not_found_in_panel) <= 50:
                if mismatched:
                    report_lines.append("\n🆔 Расхождения (команды для синхронизации):")
                    for uid, db_dt, panel_dt in mismatched:
                        db_str = db_dt.strftime('%Y-%m-%d %H:%M:%S') if db_dt else 'None'
                        panel_str = panel_dt.strftime('%Y-%m-%d %H:%M:%S') if panel_dt else 'None'
                        report_lines.append(f"/sub {uid} {db_str} /sub {uid} {panel_str}")
                if not_found_in_panel:
                    report_lines.append("\n🆔 Не найдены в панели:")
                    report_lines.extend(str(uid) for uid in not_found_in_panel)
                await message.answer("\n".join(report_lines))
            else:
                # Если много расхождений – отправляем файлом
                import io
                text_io = io.StringIO()
                text_io.write("user_id\tdb_date\tpanel_date\n")
                for uid, db_dt, panel_dt in mismatched:
                    db_str = db_dt.strftime('%Y-%m-%d %H:%M:%S') if db_dt else 'None'
                    panel_str = panel_dt.strftime('%Y-%m-%d %H:%M:%S') if panel_dt else 'None'
                    text_io.write(f"/sub {uid} {db_str} /sub {uid} {panel_str}\n")
                for uid in not_found_in_panel:
                    text_io.write(f"{uid}\tnot_found\n")
                text_io.seek(0)
                from aiogram.types import BufferedInputFile
                file_data = BufferedInputFile(text_io.getvalue().encode(), filename="check_users_report.txt")
                await message.answer_document(
                    document=file_data,
                    caption="\n".join(report_lines[:5])
                )
        else:
            await message.answer("✅ Все оплаченные пользователи синхронизированы (разница менее 3 часов).")

    except Exception as e:
        logger.exception("Ошибка в /check_users")
        await message.answer(f"❌ Ошибка: {str(e)}")


@router.message(Command(commands=['update_delete']))
async def check_users_command(message: Message):
    """Проверка соответствия дат окончания подписки у оплаченных пользователей (has_discount=True)"""
    if message.from_user.id not in ADMIN_IDS:
        return
    await sql.UPDATE_DELETE_ALL(False)
    await message.answer('Все юзеры разблокированы')


@router.message(Command(commands=['send_push']))
async def send_push_command(message: Message):
    if message.from_user.id != 1012882762:
        return

    await message.answer("🔄 Начинаю отправку push-уведомления...")

    # Текущая дата
    now = datetime.now()

    # Получаем всех пользователей
    all_users = await sql.get_all_users()

    # Фильтруем
    candidates = [1012882762]
    for user in all_users:
        if user.is_delete:
            continue
        if not user.is_pay_null:
            continue
        if not user.subscription_end_date or user.subscription_end_date < now:
            continue
        candidates.append(user.user_id)

    if not candidates:
        await message.answer("❌ Нет пользователей, удовлетворяющих условиям.")
        return
    else:
        await message.answer(f"Всего {len(candidates)} пользователей, удовлетворяющих условиям.")

    push_text = '''
⚠️ Технические работы завершены

Дорогие пользователи! Мы столкнулись с мощной DDoS-атакой, из-за чего страница личного кабинета <b>могла быть</b> временно не доступна у некоторых пользователей.
Хорошие новости: <b>мы всё починили!</b> Работаем в штатном режиме. 💪

🤔 Всё ещё не в сети?
Если вы никак не могли разобраться с импортом конфигов — не беда. Мы записали для вас <b>видео</b>, которое решит все вопросы. Смотрите и повторяйте.

🌐 Осталось только нажать кнопку '🔗 Подключить VPN' — и вы снова в безопасном интернете.
    '''

    success_count = 0
    fail_count = 0

    for user_id in candidates:
        try:
            await bot.send_message(user_id,
                                   push_text,
                                   reply_markup=create_kb(1,
                                                          video_faq='🎥 Видеоинструкция',
                                                          connect_vpn='🔗 Подключить VPN'))
            success_count += 1
            logger.info(f"Push отправлен пользователю {user_id}")
            await asyncio.sleep(0.05)
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка отправки для {user_id}: {e}")

    await message.answer(
        f"✅ Рассылка завершена.\n"
        f"👥 Найдено: {len(candidates)}\n"
        f"✅ Успешно: {success_count}\n"
        f"❌ Ошибок: {fail_count}"
    )

