import sys
from typing import Callable, Iterator

from fans.logger import get_logger

from .context import Context
from .action import Action


logger = get_logger(__name__)


def process_sync(sync: Callable[[Context], Iterator[Action]], ctx: Context):
    actions = [d for d in sync(ctx) if d.side == ctx.side]
    for i_action, action in enumerate(actions):
        action.execute()

        if 'error' in action.result:
            print(action.result['trace'], file=sys.stderr)
            logger.error(action.result['error'])
            break

        if action.result.get('data') is not None or i_action != len(actions) - 1:
            ctx.communicate(action)
