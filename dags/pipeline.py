from types import SimpleNamespace


class Pipeline(SimpleNamespace):
    def __str__(self):
        return self.data_source
