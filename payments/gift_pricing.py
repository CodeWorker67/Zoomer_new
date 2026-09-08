from lexicon import dct_price, format_gift_tariff_label, resolve_gift_price


async def gift_rub_amount_and_desc(sql, user_id: int, duration_key: str) -> tuple[int, str]:
    """Цена и описание подарочного тарифа с учётом скидки для повторных дарителей."""
    repeat_giver = await sql.is_gift_giver(user_id)
    amount = resolve_gift_price(duration_key, repeat_giver=repeat_giver)
    label = format_gift_tariff_label(duration_key, repeat_giver=repeat_giver)
    return amount, f"Подписка в подарок {label}"


def regular_rub_amount(duration_key: str) -> int:
    return dct_price[duration_key]
