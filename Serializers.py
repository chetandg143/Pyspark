import marshal

from pyspark.serializers import FramedSerializer


class MarshalSerializer(FramedSerializer):
    """
    marshal ="http://docs.python.org/2/library/marshal/html"
    """
    dumps = marshal.dumps
    loads = marshal.loads


class PickleSerializer(FramedSerializer):
    """
        http://docs.python.org/2/library/marshal/html
    """
    def dumps(self, obj):
        return cPickle.dumps(obj , 2)

    loads = cPickle.loads