import logging


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


def has_warning(caplog, prefix):
    return has_log_of_level(caplog, prefix, logging.WARNING)


def has_error(caplog, prefix):
    return has_log_of_level(caplog, prefix, logging.ERROR)


def has_log_of_level(caplog, prefix, level):
    return next((
        r for r in caplog.records if (r.levelno == level and r.message.startswith(prefix)
    )), None)
