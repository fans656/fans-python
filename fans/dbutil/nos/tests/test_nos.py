from fans.dbutil.nos import Nos


def test_simple_usage():
    nos = Nos()

    nos.put({'key': 'title', 'value': 'nos usage'})
    assert nos.get('title') == {'key': 'title', 'value': 'nos usage'}

    nos.put({'name': 'foo', 'age': 3}, collection='person')
    assert nos.get('foo', collection='person') == {'name': 'foo', 'age': 3}
    
    persons = nos.collection('person')
    persons.put({'name': 'bar', 'age': 5})
    assert persons.get('foo') == {'name': 'foo', 'age': 3}
    assert persons.get('bar') == {'name': 'bar', 'age': 5}
    
    persons.tag('foo', 'furious')
    assert persons.find('furious') == [{'name': 'foo', 'age': 3}]
