Wrap around a sqlite database and provides document store like interface.

Supporting:
- Put/Get/Delete document in a collection
- Multiple collections in single store
- Tagging document and query by tag
- Labeling document and query by label
- Nested document
- Link between documents

# Concepts

Store - corresponding to a sqlite database
Collection - corresponding to a sqlite table
Document - corresponding to a sqlite table row

# Features

## Put/Get/Delete

Sample:

    from fans.nos import nos

    nos.put({'name': 'foo', 'age': 3})
    nos.get('foo')
    nos.delete('foo')

Note:
- By default use `nos.sqlite` file in current directory
- Can be changed by `nos.path = database_path`
- You can also create custom nos instance by:

      from fans.nos import Nos

      my_nos = Nos(database_path)
      my_nos.put(...)
      my_nos.get(...)
      my_nos.delete(...)

## Collections

Sample:

    persons = nos.Collection('person')
    persons.put({'name': 'foo', 'age': 3})
    persons.get('foo')

Note:
- `nos`'s `put/get/delete` methods actually delegate to the default collection (`nos_default`)

## Tag/Label

Sample:

    nos.tag('foo', 'simple')
    nos.find({'tag': 'simple'})

    nos.label('foo', 'city', 'chengdu')
    nos.find({'label': {'city': 'chengdu'}})

## Link

Provide many to many relation between documents (in or across collections).

Sample:

    nos.link('foo', 'bar')

## Nested document

Sample:

    nos.put({
      'name': '惘闻',
      'albums': [
        {'name': '看不见的城市'},
        {'name': '岁月鸿沟'},
      ],
    }, nested='albums')

## Index

# Samples

can reset store path:

    print(nos.path)  # >>> nos.sqlite

    nos.path = 'foo.db'
    print(nos.path)  # >>> foo.db

can show collections:

    print(nos.domains)

can list docs:

    for doc in nos.list('artist'):
        print(doc)

single store can have multiple collections:

    nos.put({'name': '惘闻'}, domain='artist')
    nos.put({'name': '看不见的城市'}, domain='album')
    
    nos.get('惘闻', domain='artist')
