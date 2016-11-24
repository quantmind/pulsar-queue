import json
from collections import OrderedDict

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


class MessageMetaClass(type):

    def __new__(cls, name, bases, attrs):
        attrs['type'] = (attrs.get('type') or name).lower()
        c = super(MessageMetaClass, cls).__new__(cls, name, bases, attrs)
        message_types[c.type] = c
        return c


class Message(metaclass=MessageMetaClass):
    id = None
    """Message ID - all messages should have one"""

    @classmethod
    def consumer(cls):
        pass

    def get(self, name):
        return self.__dict__.get(name)

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
        return json.loads(data, object_hook=queue_message)

    @classmethod
    def encode(cls, message):
        return json.dumps(message, cls=JSONEncoder)


if msgpack:

    @serializer
    class MsgPack:

        @classmethod
        def decode(cls, data):
            return msgpack.unpackb(data, object_hook=queue_message,
                                   encoding='utf-8')

        @classmethod
        def encode(cls, message):
            return msgpack.packb(message, default=as_message)


def queue_message(d):
    type = d.get('type')
    MsgType = message_types.get(type)
    if MsgType:
        d.pop('type')
        return MsgType(**d)
    return d


class JSONEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, Message):
            return o.tojson()
        return super().default(o)


def as_message(o):
    return o.tojson() if isinstance(o, Message) else o
