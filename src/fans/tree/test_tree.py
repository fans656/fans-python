from fans.tree import tree


class Test_make:

    def test_default(self):
        root = tree.make({
            'name': 'foo',
            'children': [
                {'name': 'bar'},
                {'name': 'baz'},
            ],
        })
        assert root.name == 'foo'
        children = list(root.node.children)
        assert children[0].name == 'bar'
        assert children[1].name == 'baz'

    def test_wrap_using_class(self):

        class People:

            def __init__(self, data):
                self.name = data['name']

        root = tree.make({'name': 'foo'}, People)
        assert isinstance(root.data, People)

    def test_wrap_using_function(self):
        root = tree.make({'name': 'foo'}, lambda d: d['name'].upper())
        assert root.data == 'FOO'


class Test_assign_parent:

    class People:

        def __init__(self, data):
            pass

    def test_default_has_no_parent_attr(self):
        root = tree.make({}, self.People)
        assert not hasattr(root.data, 'parent')

    def test_true_set_parent_attr(self):
        root = tree.make({}, self.People, assign_parent = True)
        assert hasattr(root.data, 'parent')

    def test_str_set_parent_attr(self):
        root = tree.make({}, self.People, assign_parent = 'father')
        assert not hasattr(root.data, 'parent')
        assert hasattr(root.data, 'father')

    def test_custom_callable(self):

        def assign_parent(data, parent):
            data.foo = parent

        root = tree.make({}, self.People, assign_parent = assign_parent)
        assert not hasattr(root.data, 'parent')
        assert hasattr(root.data, 'foo')


class Test_derive:

    def test_default_derive(self, mocker):
        People = mocker.Mock()
        root = tree.make({}, People)
        root.derive(ensure_parent = False)
        root.data.derive.assert_called_once()

    def test_default_derive_with_children(self, mocker):
        People = mocker.Mock()
        root = tree.make({'children': [{}, {}]}, People)
        root.derive(ensure_parent = False)
        root.data.derive.assert_called()

    def test_str_derive(self, mocker):
        People = mocker.Mock()
        root = tree.make({}, People)
        root.derive('foo', ensure_parent = False)
        root.data.foo.assert_called_once()

    def test_callable_derive(self, mocker):
        func = mocker.Mock()
        root = tree.make({'children': [{}, {}]})
        root.derive(lambda _: func())
        func.assert_called()


class Test_derive_bottom_up:

    def test_bottom_up(self):

        class People:

            ids = []

            def __init__(self, data):
                self.id = data['id']

            def derive(self):
                self.ids.append(self.id)

        root = tree.make({
            'id': 0,
            'children': [
                {'id': 1},
            ],
        }, People)
        root.derive(bottomup = True)
        assert People.ids == [1, 0]
