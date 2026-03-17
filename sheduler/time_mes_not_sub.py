from datetime import datetime

from bot import bot, sql
from keyboard import create_kb
from lexicon import lexicon
from logging_config import logger


async def send_push_cron(debug: bool = False):
    """
    Отправляет push-уведомления пользователям с Is_tarif = False
    в определенные интервалы после создания.
    """
    try:
        # Получаем всех пользователей с Is_tarif = False
        all_users = await sql.SELECT_ALL_USERS()

        if not all_users:
            logger.info("Нет пользователей для отправки push-уведомлений")
            return

        sent_count_not_sub = 0
        failed_count_not_sub = 0
        sent_count_not_connect = 0
        failed_count_not_connect = 0
        failed_count = 0
        now = datetime.now()

        for user_id in all_users:
            try:
                # Получаем данные пользователя
                user_data = await sql.SELECT_ID(user_id)
                if not user_data:
                    continue

                create_time = user_data[6]
                if not create_time:
                    continue

                time_diff = now - create_time
                minutes_diff = time_diff.total_seconds() / 60
                video_flag = False
                if not user_data[4]: #Проверяем Is_pay_null, если нет подписки то отсылаем
                    message_text = None
                    if 30 <= minutes_diff <= 60:
                        message_text = lexicon['push_not_subscribed_30m']
                    elif 180 <= minutes_diff <= 210:
                        message_text = lexicon['push_not_subscribed_3h']
                        video_flag = True
                    elif 1410 <= minutes_diff <= 1440:
                        message_text = lexicon['push_not_subscribed_24h']

                    if message_text:
                        try:
                            keyboard_broadcast = create_kb(1, free_vpn='🔥 Попробовать бесплатно')
                            if video_flag:
                                await bot.send_video(
                                    chat_id=user_id,
                                    video='BAACAgIAAxkBAAEBk_5pmqIm8a5-5ioQ3GziIJ4dBH9PugAC_ZgAAtS92EjbvWnuAla0dDoE',
                                    caption=message_text,
                                    reply_markup=keyboard_broadcast
                                )
                            else:
                                await bot.send_message(
                                    chat_id=user_id,
                                    text=message_text,
                                    reply_markup=keyboard_broadcast
                                )
                            sent_count_not_sub += 1
                            logger.info(f"Отправлено push-уведомление пользователю {user_id}")
                        except Exception as e:
                            failed_count_not_sub += 1
                            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

                elif not user_data[5]: #Проверяем Is_tarif, если нет подключения то отсылаем
                    message_text = None
                    if 30 <= minutes_diff <= 60:
                        message_text = lexicon['push_not_connected_30m']
                    elif 180 <= minutes_diff <= 210:
                        message_text = lexicon['push_not_connected_3h']
                        video_flag = True
                    elif 1410 <= minutes_diff <= 1440:
                        message_text = lexicon['push_not_connected_24h']

                    if message_text:
                        try:
                            keyboard_broadcast = create_kb(1, connect_vpn='🔗 Подключить VPN')
                            if video_flag:
                                await bot.send_video(
                                    chat_id=user_id,
                                    video='BAACAgIAAxkBAAEBk_5pmqIm8a5-5ioQ3GziIJ4dBH9PugAC_ZgAAtS92EjbvWnuAla0dDoE',
                                    caption=message_text,
                                    reply_markup=keyboard_broadcast
                                )
                            else:
                                await bot.send_message(
                                    chat_id=user_id,
                                    text=message_text,
                                    reply_markup=keyboard_broadcast
                                )
                            sent_count_not_connect += 1
                            logger.info(f"Отправлено push-уведомление пользователю {user_id}")
                        except Exception as e:
                            failed_count_not_connect += 1
                            logger.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Ошибка обработки пользователя {user_id}: {e}")

        # Отправляем отчет администратору
        try:
            await bot.send_message(
                chat_id=1012882762,
                text=f"📊 Отчет по push-уведомлениям:\n\n"
                     f"✅ Отправлено не подписанным: {sent_count_not_sub}\n"
                     f"❌ Не удалось отправить не подписанным: {failed_count_not_sub}\n\n"
                     f"✅ Отправлено не подключенным: {sent_count_not_connect}\n"
                     f"❌ Не удалось отправить не подключенным: {failed_count_not_connect}\n\n"
                     f"❌ Не удалось обработать: {failed_count}\n\n"
                     f"⏰ Время: {now.strftime('%H:%M:%S')}"
            )
            logger.info(f"Отчет отправлен: отправлено {sent_count_not_connect + sent_count_not_sub}, не удалось {failed_count + failed_count_not_connect + failed_count_not_sub}")
        except Exception as e:
            logger.error(f"Не удалось отправить отчет: {e}")

    except Exception as e:
        logger.error(f"Критическая ошибка в send_push_cron: {e}")
