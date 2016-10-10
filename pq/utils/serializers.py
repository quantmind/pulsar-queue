from collections import OrderedDict

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


def as_message(message):
    if isinstance(message, dict):
        MsgType = message_types.get(message.get('type'))
        if MsgType:
            message.pop('type')
            message = MsgType(**message)
    return message


class MessageMetaClass(type):

    def __new__(cls, name, bases, attrs):
        c = super(MessageMetaClass, cls).__new__(cls, name, bases, attrs)
        message_types[c.__name__.lower()] = c
        return c


class Message(metaclass=MessageMetaClass):

    @property
    def type(self):
        return self.__class__.__name__.lower()

    @classmethod
    def consumer(cls):
        pass

    def tojson(self):
        '''A serializable dictionary
        '''
        data = self.__dict__.copy()
        data['type'] = self.type
        return data


@serializer
class Json:

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return as_message(json.loads(data))

    @classmethod
    def encode(cls, message):
        if isinstance(message, Message):
            message = message.tojson()
        return json.dumps(message)


if msgpack:

    @serializer
    class MsgPack:

        @classmethod
        def decode(cls, data):
            return as_message(msgpack.unpackb(data, encoding='utf-8'))

        @classmethod
        def encode(cls, message):
            if isinstance(message, Message):
                message = message.tojson()
            return msgpack.packb(message)
