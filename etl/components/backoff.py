import logging
from functools import wraps
from time import sleep


def backoff(
        start_sleep_time=0.1,
        factor=2,
        border_sleep_time=10,
        logging=logging,
):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    logging.info('Running %s', func.__name__)
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error('While running %s: %s', func.__name__, e)
                    if sleep_time >= border_sleep_time:
                        sleep_time = border_sleep_time
                    else:
                        sleep_time = min(sleep_time * factor,
                                         border_sleep_time)
                    sleep(sleep_time)

        return inner

    return func_wrapper
