import re
import struct

from fans.bunch import bunch


class Struct:

    def __init__(self, *specs):
        exp = 'name'
        fields = []
        field = None
        for i, spec in enumerate(specs):
            match exp:
                case 'name':
                    field = {'name': spec}
                    exp = 'fmt'
                    fields.append(field)
                case 'fmt':
                    field['fmt'] = spec
                    mat = fmt_regex.match(spec)
                    count = mat.group('count') or 1
                    ctype = mat.group('ctype')
                    field['size'] = int(count) * ctype_to_size[ctype]
                    exp = 'name'
        self.fields = fields
        self.size = sum(d['size'] for d in fields)
        self.fmt = ''.join(d['fmt'] for d in fields)

    def unpack(self, data: bytes):
        res = struct.unpack(self.fmt, data)
        return bunch(**{
            field['name']: value for field, value in zip(self.fields, res)
        })

    def load(self, file):
        return self.unpack(file.read(self.size))


fmt_regex = re.compile(r'(?P<count>\d+)?(?P<ctype>\w)')
ctype_to_size = {
    's': 1,
    'I': 4,
    'q': 8,
}
