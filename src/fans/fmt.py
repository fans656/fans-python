from tabulate import tabulate


def duration(seconds):
    if seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    else:
        hours, rem = divmod(int(seconds), 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"


def fmtprint(obj):
    if isinstance(obj, list):
        if not (obj and isinstance(obj[0], dict)):
            raise NotImplementedError(f"{type(obj)}")
        print(tabulate(obj, 'keys'))
    else:
        raise NotImplementedError()
