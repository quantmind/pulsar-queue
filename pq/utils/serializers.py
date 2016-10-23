from collections import OrderedDict

from pulsar import ProtocolError
from pulsar.utils.system import json

try:
    import msgpack
except ImportError:     # pragma    nocover
    msgpack = None


serializers = OrderedDict()
message_types = {}


def serializer(cls):
    name = cls.__name__.lower()
    serializers[name] = cls
    return cls


def load_message(message):
    if isinstance(message, dict):
        MsgType = message_types.get(message.pop('type', None))
        if MsgType:
            return MsgType(**message)


def as_message(message):
    msg = load_message(message.get('data'))
    if msg:
        message['data'] = msg
    else:
        message = load_message(message)
        if not message:
            raise ProtocolError
    return message


def tojson(message):
    if isinstance(message, dict):
        msg = message.get('data')
        if isinstance(msg, Message):
            message['data'] = msg.tojson()
    elif isinstance(message, Message):
        message = message.tojson()
    return message


class MessageMetaClass(type):

    def __new__(cls, name, bases, attrs):
        attrs['type'] = (attrs.get('type') or name).lower()
        c = super(MessageMetaClass, cls).__new__(cls, name, bases, attrs)
        message_types[c.type] = c
        return c


class Message(metaclass=MessageMetaClass):

    @classmethod
    def consumer(cls):
        pass

    def tojson(self):
        '''A serializable dictionary
        '''
        data = self.__dict__.copy()
        data['type'] = self.type
        return data


class MessageDict(Message):

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)


@serializer
class Json:

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return as_message(json.loads(data))

    @classmethod
    def encode(cls, message):
        return json.dumps(tojson(message))


if msgpack:

    @serializer
    class MsgPack:

        @classmethod
        def decode(cls, data):
            return as_message(msgpack.unpackb(data, encoding='utf-8'))

        @classmethod
        def encode(cls, message):
            return msgpack.packb(tojson(message))
