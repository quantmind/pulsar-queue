from . import redis
from ..api import register_broker

register_broker('redis', redis.MQ)
