"""Tests task execution with MsgPack serialiser"""
import unittest

try:
    import msgpack
except ImportError:
    msgpack = None


from tests import app


@unittest.skipUnless(msgpack, "Requires msgpack library")
class TestMsgPackQueue(app.TaskQueueApp, unittest.TestCase):
    message_serializer = 'msgpack'
