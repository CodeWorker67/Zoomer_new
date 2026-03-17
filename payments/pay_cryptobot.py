import aiohttp
from typing import Dict, Optional
from aiogram import F, Router
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from bot import sql
from config import CRYPTOBOT_API_TOKEN, ADMIN_IDS
from keyboard import create_kb
from lexicon import lexicon
from logging_config import logger

router: Router = Router()


class CryptoBotPayment:
    """Класс для взаимодействия с Cryptobot API"""
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://pay.crypt.bot/api"
        self.headers = {
            "Crypto-Pay-API-Token": api_token,
            "Content-Type": "application/json"
        }

    async def create_invoice(self, asset: str, amount: float, description: str,
                             payload: str, expires_in: int = 7200) -> Dict:
        """Создание счета в Cryptobot"""
        url = f"{self.base_url}/createInvoice"
        data = {
            "asset": asset,
            "amount": str(amount),
            "description": description,
            "payload": payload,
            "paid_btn_name": "openBot",
            "paid_btn_url": "https://t.me/zoomerskyvpn_bot",
            "allow_comments": False,
            "allow_anonymous": False,
            "expires_in": expires_in
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=self.headers) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("ok"):
                            inv = result["result"]
                            return {
                                'status': 'pending',
                                'url': inv['pay_url'],
                                'invoice_id': inv['invoice_id'],
                                'payload': payload
                            }
                        else:
                            logger.error(f"Cryptobot API error: {result}")
                            return {'status': 'error', 'message': result.get('error')}
                    else:
                        text = await resp.text()
                        logger.error(f"Cryptobot HTTP error {resp.status}: {text}")
                        return {'status': 'error', 'message': f"HTTP {resp.status}"}
        except Exception as e:
            logger.error(f"Error creating Cryptobot invoice: {e}")
            return {'status': 'error', 'message': str(e)}

    async def get_invoice_status(self, invoice_id: int) -> Optional[str]:
        """Получение статуса счета по invoice_id"""
        url = f"{self.base_url}/getInvoices"
        params = {"invoice_ids": str(invoice_id)}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("ok") and result.get("result", {}).get("items"):
                            invoice = result["result"]["items"][0]
                            return invoice.get("status")
                        else:
                            logger.error(f"Failed to get invoice {invoice_id}: {result}")
                            return None
                    else:
                        logger.error(f"HTTP error {resp.status} for invoice {invoice_id}")
                        return None
        except Exception as e:
            logger.error(f"Error checking invoice {invoice_id}: {e}")
            return None


async def create_cryptobot_payment(amount: float, currency: str, description: str,
                                   user_id: int, duration: int, white: bool,
                                   is_gift: bool, method: str) -> Dict:
    """
    Создание платежа через Cryptobot и запись в БД.
    Возвращает словарь с ключами: status, url, invoice_id.
    """
    cryptobot = CryptoBotPayment(CRYPTOBOT_API_TOKEN)

    # Формируем payload для последующей обработки
    payload = (f"user_id:{user_id},duration:{duration},white:{white},"
               f"gift:{is_gift},method:{method},amount:{amount}")

    result = await cryptobot.create_invoice(
        asset=currency,
        amount=amount,
        description=description,
        payload=payload
    )
    if result['status'] == 'pending':
        # Сохраняем платёж в БД через AsyncSQL
        try:
            await sql.add_cryptobot_payment(
                user_id=user_id,
                amount=amount,
                currency=currency,
                is_gift=is_gift,
                invoice_id=result['invoice_id'],
                payload=payload
            )
        except Exception as e:
            logger.error(f"Error saving cryptobot payment to DB: {e}")
            return {'status': 'error', 'url': '', 'invoice_id': ''}

    return result


def get_crypto_amount(currency: str, duration: str) -> float:
    """Возвращает цену для тарифа в указанной криптовалюте"""
    prices = {
        'TON': {'30': 0.9, '90': 2.5, '120': 2.5, '180': 4.6, 'white_30': 2.8},
        'USDT': {'30': 1.3, '90': 3.5, '120': 3.5, '180': 6.5, 'white_30': 4.0}
    }
    return prices.get(currency.upper(), {}).get(duration, 0)


# Обработчик для крипто-оплаты
@router.callback_query(F.data.startswith('crypto_'))
async def process_payment_crypto(callback: CallbackQuery):
    gift_flag = False
    white_flag = False
    data = callback.data

    parts = data.split('_')
    currency = parts[1].upper()

    if 'gift_' in data:
        gift_flag = True

    if gift_flag:
        duration = data.replace(f'crypto_{parts[1]}_gift_r_', '')
    else:
        duration = data.replace(f'crypto_{parts[1]}_r_', '')

    crypto_amount = get_crypto_amount(currency, duration)

    if 'white' in duration:
        white_flag = True
        duration = duration.replace('white_', '')

    if not crypto_amount:
        await callback.answer("Ошибка определения цены для данной валюты", show_alert=True)
        return

    if callback.from_user.id in ADMIN_IDS:
        crypto_amount = 0.02

    result = await create_cryptobot_payment(
        amount=crypto_amount,
        currency=currency,
        description=f"Подписка VPN {duration} дней",
        user_id=callback.from_user.id,
        duration=int(duration),
        white=white_flag,
        is_gift=gift_flag,
        method=currency.lower()
    )

    if result['status'] == 'pending':
        if white_flag:
            text = lexicon.get('payment_link_white', 'Оплата в {0}: {1}').format(currency, crypto_amount)
        else:
            text = lexicon.get('payment_link', 'Оплата в {0}: {1}').format(currency, crypto_amount)
        pay_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Оплатить {crypto_amount} {currency}", url=result['url'])]
        ])
        await callback.message.edit_text(text, reply_markup=pay_keyboard)
    else:
        await callback.message.answer(
            lexicon.get('error_payment', 'Произошла ошибка при создании счета.'),
            reply_markup=create_kb(1, back_to_main='🔙 Назад')
        )
