import re
import inspect
from typing import Callable
import ctypes
from ctypes import wintypes

import win32con
import win32gui


__all__ = [
    'global_hotkey_enabled',
]


def global_hotkey_enabled(qwidget_class):
    def method(func):
        func.super = getattr(qwidget_class, func.__name__, None)
        setattr(qwidget_class, func.__name__, func)
        return func

    @method
    def nativeEvent(self, eventType, message):
        if eventType == 'windows_generic_MSG':
            msg_ptr = ctypes.cast(int(message), ctypes.POINTER(MSG))
            msg = msg_ptr.contents
            if msg.message == win32con.WM_HOTKEY:
                hotkey_id = msg.wParam
                callback = self.__hotkey_id_to_callback.get(hotkey_id)
                if callback:
                    callback()
        return nativeEvent.super(self, eventType, message)

    @method
    def __init__(self, *args, **kwargs):
        self.__next_hotkey_id = 1
        self.__modifier_key_to_hotkey_id = {}
        self.__hotkey_id_to_callback = {}
        __init__.super(self, *args, **kwargs)

    @method
    def register_global_hotkey(self, sequence: str, callback: Callable[[], None]):
        modifier, key = _modifier_key_from_sequence(sequence)
        hotkey_id = self.__hotkey_id_from_modifier_key(modifier, key)
        if hotkey_id:
            self.__unregister_global_hotkey(hotkey_id)
        else:
            hotkey_id = self.__modifier_key_to_hotkey_id[(modifier, key)] = self.__next_hotkey_id
            self.__next_hotkey_id += 1

        self.__hotkey_id_to_callback[hotkey_id] = callback

        win32gui.RegisterHotKey(self.winId(), hotkey_id, modifier, key)

    @method
    def unregister_global_hotkey(self, sequence):
        if hotkey_id := self.__hotkey_id_from_sequence(sequence):
            self.__unregister_global_hotkey(hotkey_id)

    @method
    def has_global_hotkey(self, sequence):
        return _modifier_key_from_sequence(sequence) in self.__modifier_key_to_hotkey_id

    @method
    def __unregister_global_hotkey(self, hotkey_id):
        win32gui.UnregisterHotKey(self.winId(), hotkey_id)
        modifier_key = next(
            (mk for mk, hk in self.__modifier_key_to_hotkey_id.items() if hk == hotkey_id),
            None,
        )
        del self.__modifier_key_to_hotkey_id[modifier_key]

    @method
    def __hotkey_id_from_sequence(self, sequence):
        return self.__hotkey_id_from_modifier_key(*_modifier_key_from_sequence(sequence))

    @method
    def __hotkey_id_from_modifier_key(self, modifier, key):
        return self.__modifier_key_to_hotkey_id.get((modifier, key))

    return qwidget_class


class MSG(ctypes.Structure):
    _fields_ = [
        ("hwnd", wintypes.HWND),
        ("message", wintypes.UINT),
        ("wParam", wintypes.WPARAM),
        ("lParam", wintypes.LPARAM),
        ("time", wintypes.DWORD),
        ("pt", wintypes.POINT),
    ]


def _modifier_key_from_sequence(sequence):
    key_names = re.sub('[+-]', ' ', sequence).upper().split()
    key = ord(key_names[-1])

    modifier = 0x0
    key_names = set(key_names)
    if 'CTRL' in key_names:
        modifier |= win32con.MOD_CONTROL
    if 'SHIFT' in key_names:
        modifier |= win32con.MOD_SHIFT
    if 'ALT' in key_names:
        modifier |= win32con.MOD_ALT

    return modifier, key


if __name__ == '__main__':
    from PySide6.QtWidgets import QApplication, QWidget


    @global_hotkey_enabled
    class Widget(QWidget):

        def __init__(self):
            super().__init__()

            self.hotkeys = {
                'toggle_visible': 'ctrl alt v',
                'toggle_hotkey': 'ctrl alt d'
            }

            self.register_global_hotkey(self.hotkeys['toggle_visible'], self.toggle_visible)
            self.register_global_hotkey(self.hotkeys['toggle_hotkey'], self.toggle_hotkey)

        def toggle_visible(self):
            if self.isVisible():
                self.hide()
            else:
                self.show()

        def toggle_hotkey(self):
            hotkey = self.hotkeys['toggle_visible']
            if self.has_global_hotkey(hotkey):
                self.unregister_global_hotkey(hotkey)
            else:
                self.register_global_hotkey(hotkey, self.toggle_visible)


    app = QApplication([])

    widget = Widget()
    widget.show()

    app.exec()
