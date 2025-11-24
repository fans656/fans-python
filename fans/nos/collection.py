import json


class Collection:

    def __init__(self, name, Model, Tag, Label):
        self.name = name
        
        self.Model = Model
        self.Tag = Tag
        self.Label = Label

    def put(self, doc):
        self.Model.insert(
            key=self._get_doc_key(doc),
            value=json.dumps(doc),
        ).execute()

    def get(self, key):
        M = self.Model
        model = M.get_or_none(M.key == key)
        if model is None:
            return None
        return json.loads(model.value)

    def delete(self, key):
        M = self.Model
        M.delete().where(M.key == key).execute()

    def label(self, doc_key, labels: dict):
        self.Label.insert_many([{
            'doc_key': doc_key,
            'label_key': label_key,
            'label_value': label_value,
        } for label_key, label_value in labels.items()]).on_conflict_ignore().execute()

    def tag(self, doc_key, *tags):
        self.Tag.insert_many([{
            'doc_key': doc_key,
            'tag': tag,
        } for tag in tags]).on_conflict_ignore().execute()

    def search(self, query: dict):
        if 'label' in query:
            Model = self.Model
            Label = self.Label

            pred = True
            for label_key, label_value in query['label'].items():
                pred &= (Label.label_key == label_key) & (Label.label_value == label_value)

            query = Model.select(Model.value).where(
                Model.key << Label.select(Label.doc_key).where(pred)
            ).order_by(Model.key)
            docs = [json.loads(d.value) for d in query]
            return docs
        elif 'tag' in query:
            Model = self.Model
            Tag = self.Tag

            tags = query['tag']
            if isinstance(tags, str):
                tags = [tags]

            pred = True
            for tag in tags:
                pred &= Tag.tag == tag

            query = Model.select(Model.value).where(
                Model.key << Tag.select(Tag.doc_key).where(pred)
            ).order_by(Model.key)
            docs = [json.loads(d.value) for d in query]
            return docs
        else:
            raise NotImplementedError(f'search {query}')

    def list(self):
        Model = self.Model
        for item in Model.select(Model.value).order_by(Model.key):
            yield json.loads(item.value)
    
    def __iter__(self):
        yield from self.list()

    def __len__(self):
        return self.Model.select().count()

    def _get_doc_key(self, doc):
        for key in ['id', 'key', 'name']:
            value = doc.get(key)
            if value is not None:
                return value
        raise ValueError(f'doc {doc} lacks nos key')

