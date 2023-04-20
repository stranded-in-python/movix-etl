import logging
import traceback

from time import sleep

from functools import wraps
from typing import Callable, Optional, Any


def backoff(
    func: Callable[..., Any],
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
    logger: Optional[logging.Logger] = None
):
    """
    Декоратор для обработки ошибок выполнения вызываемого объекта.
    Повторяет выполнение func через указанное время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                            level=logging.INFO
                            )

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        failures = 0

        while True:
            try:
                return func(*args, **kwargs)

            except Exception:
                logger.error(traceback.format_exc())

                sleep(min(border_sleep_time,
                          start_sleep_time * factor ** failures)
                      )
                failures += 1

    return func_wrapper
