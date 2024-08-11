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


def human_size(size):
    if size > TB:
        return f'{size / TB:.1f}T'
    elif size > GB:
        return f'{size / GB:.1f}G'
    elif size > MB:
        return f'{size / MB:.1f}M'
    elif size > KB:
        return f'{size / KB:.1f}K'
    else:
        return f'{size}B'


KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB
