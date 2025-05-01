import functools
from pathlib import Path

from .store import Store
from .collection import Collection
from .cons import DEFAULT_DOMAIN


class Nos:

    def __init__(self, path: str = 'nos.sqlite'):
        self._path = Path(path)
        self._store = None

    @property
    def path(self):
        """Return current database path"""
        return self._path

    @path.setter
    def path(self, path):
        """Set current database path"""
        self._path = Path(path)
        self._store = None

    @property
    def store(self):
        """Return current Store"""
        if self._store is None:
            self._store = Store(self._path)
        return self._store

    def put(self, doc, domain=DEFAULT_DOMAIN):
        """Put a doc into store"""
        return self._get_collection(domain).put(doc)

    def get(
            self,
            doc_id: str,
            domain: str = DEFAULT_DOMAIN,
            fields: list[str] = [],
    ):
        """Get a doc from store by doc ID"""
        doc = self._get_collection(domain).get(doc_id)
        if doc is None:
            return None

        if fields:
            store = self.store
            for field in fields:
                link_domains = store.get_field_link(field)
                if not link_domains:
                    continue
                src_domain, dst_domain = link_domains
                assert src_domain == domain
                Link = store.get_link_model(src_domain, dst_domain)
                query = Link.select(
                    Link.dst,
                ).where(
                    (Link.src == doc_id)
                    & (Link.rel == field)
                )
                doc[field] = [self.get(d.dst, domain=dst_domain) for d in query]

        return doc

    def delete(self, key: str, domain=DEFAULT_DOMAIN):
        return self._get_collection(domain).delete(key)

    def domain(self, *args, **kwargs) -> Collection:
        """Get a domain (collection) instance"""
        return self.store.get_collection(*args, **kwargs)

    @property
    def domains(self):
        return [name for name in self.store.database.get_tables() if not name.startswith('__')]

    def label(self, doc_key, labels: dict, domain=DEFAULT_DOMAIN):
        """Add labels to a doc"""
        return self._get_collection(domain).label(doc_key, labels)

    def tag(self, doc_key, *tags, domain=DEFAULT_DOMAIN):
        """Add tags to a doc"""
        return self._get_collection(domain).tag(doc_key, *tags)

    def search(self, query: dict, domain=DEFAULT_DOMAIN):
        """Search doc (by label/tag)"""
        return self._get_collection(domain).search(query)

    def list(self, domain=DEFAULT_DOMAIN):
        return self._get_collection(domain).list()

    def link(
            self,
            src: str|tuple[str, str],
            dst: str|tuple[str, str],
            rel: str = '',
            field: bool = False,
    ):
        src_domain, src_doc_id = _normalized_link_node(src)
        dst_domain, dst_doc_id = _normalized_link_node(dst)

        store = self.store
        if field:
            store.ensure_field_link(rel, src_domain, dst_domain)

        Link = store.get_link_model(src_domain, dst_domain)
        Link.insert(
            src=src_doc_id,
            dst=dst_doc_id,
            rel=rel,
        ).execute()

    def _get_collection(self, domain=DEFAULT_DOMAIN):
        return self.store.get_collection(domain)


def _normalized_link_node(link_node: str|tuple[str, str]):
    if isinstance(link_node, str):
        return DEFAULT_DOMAIN, link_node
    else:
        return link_node
