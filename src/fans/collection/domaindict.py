import itertools


class DomainDict:

    def __init__(self, init_dict = None, domains: list[str] = ()):
        self._domain_to_dict = {default_domain: dict(init_dict or {})}
        self._domains = list(domains)
        for domain in domains:
            self._domain_to_dict[domain] = {}

    def reorder(self, *domains):
        """
        reorder('1st', '2nd', ..., 'kth') -> ['1st', '2nd', '3th', '4th', 'kth']
        """
        heads = list(itertools.takewhile(lambda d: d != ..., domains))
        tails = list(itertools.takewhile(lambda d: d != ..., reversed(domains)))
        self._domains = [
            *heads,
            *[d for d in self._domains if d not in {*heads, *tails}],
            *tails,
        ]

    def get(self, key, domain: str = None):
        if domain is None:
            return self._find(key, raises = False)
        else:
            return self._domain_to_dict[domain][key]

    def __getitem__(self, key):
        domain, key = self._key_to_domain_key(key, check_all = True)
        if domain is None:
            return self._find(key, raises = True)
        else:
            return self._domain_to_dict[domain][key]

    def __setitem__(self, key, value):
        domain, key = self._key_to_domain_key(key)
        if domain not in self._domain_to_dict:
            self._domain_to_dict[domain] = {}
            self._domains.append(domain)
        self._domain_to_dict[domain][key] = value

    def __delitem__(self, key):
        domain, key = self._key_to_domain_key(key, check_all = True)
        if domain is None:
            for dct in self._domain_to_dict.values():
                dct.pop(key, None)
        else:
            dct = self._domain_to_dict.get(domain)
            dct.pop(key, None)

    def _key_to_domain_key(self, key, check_all = False):
        if isinstance(key, slice):
            return key.start, key.stop
        else:
            return (None if check_all else default_domain), key

    def _find(self, key, raises = False):
        for domain in (*self._domains, default_domain):
            dct = self._domain_to_dict.get(domain)
            if dct:
                value = dct.get(key)
                if value:
                    return value
        if raises:
            raise KeyError(key)
        else:
            return None


default_domain = id({})
