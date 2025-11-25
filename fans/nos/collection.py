import json
from typing import Callable


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


def default_item_id_equal(Item, item_id):
    return Item.id == item_id


class Collection:

    def __init__(
            self,
            name,
            *,
            Item,
            Tag,
            Label,
            get_item_id: Callable[[dict], any] = default_get_item_id,
            insert_dict_from_item: Callable[['Collection', dict], dict] = default_insert_dict_from_item,
            item_id_equal: Callable[['Item', any], bool] = default_item_id_equal,
    ):
        self.name = name
        
        self.Item = Item
        self.Tag = Tag
        self.Label = Label
        
        self._get_item_id = get_item_id
        self._insert_dict_from_item = insert_dict_from_item
        self._item_id_equal = item_id_equal

    def put(self, item: dict):
        insert_dict = self._insert_dict_from_item(item, collection=self)
        self.Item.insert(insert_dict).execute()

    def get(self, item_id):
        Item = self.Item
        item = Item.get_or_none(self._item_id_equal(Item, item_id))
        return json.loads(item.data) if item else None

    def delete(self, item_id):
        Item = self.Item
        Item.delete().where(self._item_id_equal(Item, item_id)).execute()

    def label(self, item_id, labels: dict):
        self.Label.insert_many([{
            'item_id': item_id,
            'label_key': label_key,
            'label_value': label_value,
        } for label_key, label_value in labels.items()]).on_conflict_ignore().execute()

    def tag(self, item_id, *tags):
        self.Tag.insert_many([{
            'item_id': item_id,
            'tag': tag,
        } for tag in tags]).on_conflict_ignore().execute()

    def search(self, query: dict):
        if 'label' in query:
            Item = self.Item
            Label = self.Label

            pred = True
            for label_key, label_value in query['label'].items():
                pred &= (Label.label_key == label_key) & (Label.label_value == label_value)

            query = Item.select(Item.data).where(
                Item.id << Label.select(Label.item_id).where(pred)
            ).order_by(Item.id)
            items = [json.loads(d.data) for d in query]
            return items
        elif 'tag' in query:
            Item = self.Item
            Tag = self.Tag

            tags = query['tag']
            if isinstance(tags, str):
                tags = [tags]

            pred = True
            for tag in tags:
                pred &= Tag.tag == tag

            query = Item.select(Item.data).where(
                Item.id << Tag.select(Tag.item_id).where(pred)
            ).order_by(Item.id)
            items = [json.loads(d.data) for d in query]
            return items
        else:
            raise NotImplementedError(f'search {query}')

    def list(self):
        Item = self.Item
        for item in Item.select(Item.data).order_by(Item.id):
            yield json.loads(item.data)
    
    def __iter__(self):
        yield from self.list()

    def __len__(self):
        return self.Item.select().count()
