import re
import shlex


def parse_query_expr(expr: str):
    expr = re.sub(r'([!()&|])', r' \1 ', expr)
    tokens = shlex.split(expr)
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
