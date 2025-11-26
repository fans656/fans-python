import json
from typing import Callable

from fans import dbutil


def default_get_item_id(item: dict):
    for id_key in ['id', 'key', 'name']:
        value = item.get(id_key)
        if value is not None:
            return value
    raise ValueError(f'item {item} lacks nos id')


def default_insert_dict_from_item(item: dict, *, collection):
    return {
        'id': collection._get_item_id(item),
        'data': json.dumps(item),
    }


def default_item_id_equal(model, item_id):
    return model.id == item_id


class Collection:

    def __init__(
            self,
            name,
            *,
            database,
            model,
            Label,
            get_item_id: Callable[[dict], any] = default_get_item_id,
            insert_dict_from_item: Callable[['Collection', dict], dict] = default_insert_dict_from_item,
            item_id_equal: Callable[['Item', any], bool] = default_item_id_equal,
    ):
        self.name = name
        self.database = database
        
        self.model = model
        self.Label = Label
        
        self.__tagging = None

        self._get_item_id = get_item_id
        self._insert_dict_from_item = insert_dict_from_item
        self._item_id_equal = item_id_equal

    def put(self, item: dict):
        insert_dict = self._insert_dict_from_item(item, collection=self)
        self.model.insert(insert_dict).execute()

    def get(self, item_id):
        model = self.model
        item = model.get_or_none(self._item_id_equal(model, item_id))
        return json.loads(item.data) if item else None

    def delete(self, item_id):
        model = self.model
        model.delete().where(self._item_id_equal(model, item_id)).execute()

    def label(self, item_id, labels: dict):
        self.Label.insert_many([{
            'item_id': item_id,
            'label_key': label_key,
            'label_value': label_value,
        } for label_key, label_value in labels.items()]).on_conflict_ignore().execute()

    def tag(self, item_id, *tags):
        self._tagging.add_tag(item_id, *tags)

    def find(self, query: dict):
        if 'label' in query:
            return self.find_by_label(query['label'])
        elif 'tag' in query:
            return self.find_by_tag(query['tag'])
        else:
            raise NotImplementedError(f'find {query}')

    def find_by_label(self, labels: dict):
        model = self.model
        Label = self.Label

        pred = True
        for label_key, label_value in labels.items():
            pred &= (Label.label_key == label_key) & (Label.label_value == label_value)

        query = model.select(model.data).where(
            model.id << Label.select(Label.item_id).where(pred)
        ).order_by(model.id)
        items = [json.loads(d.data) for d in query]
        return items

    def find_by_tag(self, query: str):
        model = self.model
        item_ids = self._tagging.find(query, return_query=True)
        # TODO: handle composite key
        query = model.select(model.data).where(model.id << item_ids).order_by(model.id)
        items = [json.loads(d.data) for d in query]
        return items

    def list(self):
        model = self.model
        for item in model.select(model.data).order_by(model.id):
            yield json.loads(item.data)
    
    @property
    def _tagging(self):
        if self.__tagging is None:
            self.__tagging = dbutil.tagging(self.database, f'{self.name}_tag', target=self.model)
        return self.__tagging

    def __iter__(self):
        yield from self.list()

    def __len__(self):
        return self.model.select().count()
