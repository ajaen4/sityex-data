from typing import Any


def is_number(s: Any) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False
