class Meta:
    """
    Construct pytest test class from given testcase specs.

    See eno.tests.test_router for some examples.
    """

    def __new__(cls, name, bases, attrs):
        if 'testcases' in attrs:
            make_testcase = attrs.get('make_testcase')
            if make_testcase is None:
                raise RuntimeError(f'require `make_testcase` method in `{name}` with `testcases`')
            for testcase in attrs['testcases']:
                method_name = 'test_' + testcase['name'].replace(' ', '_')
                attrs[method_name] = make_testcase(testcase)
        return type(name, bases, attrs)
