"""Парсинг Telegram /start вида YD{stamp}_{yandex_id} (префикс YD — заглавные)."""

from __future__ import annotations

from typing import Optional, Tuple


def parse_yd_start_arg(start_arg: str) -> Optional[Tuple[str, str]]:
    """
    YDhapp_lab_1791285975313217366 → stamp=happ_lab, yandex_id=1791285975313217366
    stamp — между YD и последним «_», yandex_id — после последнего «_».
    """
    if not start_arg or not start_arg.startswith("YD"):
        return None
    rest = start_arg[2:]
    if "_" not in rest:
        return None
    stamp, yandex_id = rest.rsplit("_", 1)
    stamp = stamp.strip()
    yandex_id = yandex_id.strip()
    if not stamp or not yandex_id:
        return None
    return stamp[:100], yandex_id[:100]
