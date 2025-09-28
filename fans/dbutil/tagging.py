import shlex
import operator
import functools
import itertools

import peewee


class tagging:
    
    def __init__(
        self,
        database,
        table_name: str = 'tag',
        key_type: type = str,
    ):
        self.database = database
        self.table_name = table_name
        self.key_type = key_type
        
        self.model = _make_model(database, table_name, key_type)
        
        self.database.bind([self.model])
        self.database.create_tables([self.model])
    
    def add_tag(self, keys_or_key, *tags):
        if isinstance(keys_or_key, (list, tuple)):
            keys = keys_or_key
        else:
            keys = [keys_or_key]
        
        items = itertools.product(keys, tags)
        
        self.model.insert_many(items).on_conflict_ignore().execute()
    
    def find(self, expr: str):
        m = self.model
        query = m.select(m.key)

        res = _parse_query_expr(expr)

        if res['has_or'] or res['has_and'] or res['has_not']:
            tree = res['tree']
            if res['has_or'] and not (res['has_and'] or res['has_not']):  # simple OR expr
                query = query.where(m.tag << tree['subs'])
            else:  # complex expr
                query = query.group_by(m.key).having(_tree_to_having_cond(tree, m))
        else:  # single tag query
            query = query.where(m.tag == expr)

        return [d.key for d in query]
    
    def tags(self, key):
        m = self.model
        query = m.select(m.tag).where(m.key == key)
        return [d.tag for d in query]


def _make_model(database, table_name, key_type):

    class Meta:
        
        primary_key = peewee.CompositeKey('key', 'tag')
    
    if key_type is str:
        key_field = peewee.TextField()
    elif key_type is int:
        key_field = peewee.IntegerField()
    else:
        raise ValueError(f'unsupported key type {key_type}')

    cls_body = {
        'Meta': Meta,
        'key': key_field,
        'tag': peewee.TextField(index=True),
    }

    return type(table_name, (peewee.Model,), cls_body)


def _parse_query_expr(expr: str):
    tokens = list(shlex.shlex(expr, posix=True, punctuation_chars=True))
    tokens = _normalized_tokens(tokens)
    parser = Parser(tokens)
    tree = parser.parse()
    return {
        'tokens': tokens,
        'tree': tree,
        **parser.info,
    }


def _normalized_tokens(tokens):
    ret = []
    n = len(tokens)
    _is_value = lambda d: d not in '!&|()'
    for i, token in enumerate(tokens):
        ret.append(token)
        if i + 1 < n:
            if _is_value(token) or token == ')':
                next_token = tokens[i + 1]
                if _is_value(next_token) or next_token == '(' or next_token == '!':
                    ret.append('&')
    return ret


def _tree_to_having_cond(tree, m):
    if isinstance(tree, str):
        return peewee.fn.sum(m.tag == tree) == 1
    elif isinstance(tree, dict):
        conds = [_tree_to_having_cond(sub, m) for sub in tree['subs']]
        match tree['type']:
            case 'and':
                return functools.reduce(operator.and_, conds)
            case 'or':
                return functools.reduce(operator.or_, conds)
            case 'not':
                return ~conds[0]
            case _:
                raise ValueError(f"Unknown operator type: {op_type}")
    else:
        raise TypeError(f"Invalid tree node type: {type(tree)}")


class Parser:

    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0
        self.info = {
            'has_or': False,
            'has_and': False,
            'has_not': False,
        }

    def parse(self):
        return self.expr()

    def expr(self):
        return self.or_expr()

    def or_expr(self):
        node = self.and_expr()
        while self.match('|'):
            self.info['has_or'] = True
            right = self.and_expr()
            if isinstance(node, dict) and node['type'] == 'or':
                subs = node['subs']
            else:
                subs = [node]
            node = {'type': 'or', 'subs': [*subs, right]}
        return node

    def and_expr(self):
        node = self.not_expr()
        while self.match('&'):
            self.info['has_and'] = True
            right = self.not_expr()
            if isinstance(node, dict) and node['type'] == 'and':
                subs = node['subs']
            else:
                subs = [node]
            node = {'type': 'and', 'subs': [*subs, right]}
        return node

    def not_expr(self):
        if self.match('!'):
            self.info['has_not'] = True
            return {'type': 'not', 'subs': [self.not_expr()]}
        return self.atom()

    def atom(self):
        if self.match('('):
            node = self.expr()
            self.expect(')')
            return node
        token = self.consume()
        return token

    def match(self, expected):
        if self.pos < len(self.tokens) and self.tokens[self.pos] == expected:
            self.pos += 1
            return True
        return False

    def expect(self, expected):
        if not self.match(expected):
            raise SyntaxError(f"Expected '{expected}' at position {self.pos}")

    def consume(self):
        if self.pos >= len(self.tokens):
            raise SyntaxError("Unexpected end of input")
        token = self.tokens[self.pos]
        self.pos += 1
        return token
