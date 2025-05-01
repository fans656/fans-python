Providing a document store which support:
- put document into collection
- get document by custom id
- multiple collections in single store
- tagging document and query by tag
- labeling document and query by label
- nested document
- link between documents

# Concepts

Store - a sqlite database to store documents
Collection - a sqlite table to store documents of same type
Document - a JSON object that can store arbitrary fields

# Features

## Put/Get/Delete

Sample:

    nos.put({'name': 'foo', 'age': 3})
    nos.get('foo')
    nos.delete('foo')

## Collections

Sample:

    persons = nos.Collection('person')
    persons.put({'name': 'foo', 'age': 3})
    persons.get('foo')

## Tag/Label

Sample:

    nos.tag('foo', 'simple')
    nos.search({'tag': 'simple'})

    nos.label('foo', 'city', 'chengdu')
    nos.search({'label': {'city': 'chengdu'}})

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
