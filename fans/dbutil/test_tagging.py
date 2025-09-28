import peewee

from fans import dbutil


def test_all():
    """
    -----------------------------------------------------------------
    0   even                    square  cube
    -----------------------------------------------------------------
    1           odd             square  cube                factorial
    -----------------------------------------------------------------
    2   even            prime                               factorial
    -----------------------------------------------------------------
    3           odd     prime
    -----------------------------------------------------------------
    4   even                    square
    -----------------------------------------------------------------
    5           odd     prime
    -----------------------------------------------------------------
    6   even                                    perfect     factorial
    -----------------------------------------------------------------
    7           odd     prime
    -----------------------------------------------------------------
    8   even                            cube
    -----------------------------------------------------------------
    9           odd             square
    -----------------------------------------------------------------
    """
    db = peewee.SqliteDatabase(':memory:')

    tagging = dbutil.tagging(db, key_type=int)

    tagging.add_tag([0, 2, 4, 6, 8], 'even')
    tagging.add_tag([1, 3, 5, 7, 9], 'odd')
    tagging.add_tag([2, 3, 5, 7], 'prime')
    tagging.add_tag([0, 1, 4, 9], 'square')
    tagging.add_tag([0, 1, 8], 'cube')
    tagging.add_tag(6, 'perfect')
    tagging.add_tag([1, 2, 6], 'factorial')
    
    # single tag expr
    assert set(tagging.find('prime')) == {2,3,5,7}

    # simple OR expr
    assert set(tagging.find('cube | square')) == {0,1,4,8,9}

    # simple AND expr
    assert set(tagging.find('prime factorial')) == {2}
    
    # complex
    assert set(tagging.find('(cube | square) even')) == {0,4,8}
    assert set(tagging.find('odd (cube | square)')) == {1,9}
    assert set(tagging.find('even !factorial !cube')) == {4}
    
    
    # test get tags
    assert set(tagging.tags(0)) == {'even', 'square', 'cube'}
    assert set(tagging.tags(1)) == {'odd', 'square', 'cube', 'factorial'}
    assert set(tagging.tags(6)) == {'even', 'perfect', 'factorial'}
