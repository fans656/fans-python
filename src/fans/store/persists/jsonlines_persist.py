import jsonlines


class Persist:

    def open(self, path, *args, **kwargs):
        return jsonlines.open(path, *args, **kwargs)
