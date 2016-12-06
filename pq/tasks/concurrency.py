import math
from multiprocessing import cpu_count


MULTIPLIER_NAME = 'max_concurrent_task_multiplier'


def linear(cfg):
    multiplier = cfg.get(MULTIPLIER_NAME, 2)
    return multiplier*cpu_count()


def log(cfg):
    multiplier = cfg.get(MULTIPLIER_NAME, 5)
    return 1 + round(multiplier*math.log(cpu_count()))
