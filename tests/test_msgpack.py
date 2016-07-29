"""Tests task execution with MsgPack serialiser"""
import unittest

from tests import app


class TestMsgPackQueue(app.TaskQueueApp, unittest.TestCase):
    message_serializer = 'msgpack'
