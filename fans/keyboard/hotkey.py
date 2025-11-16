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


class MSG(ctypes.Structure):
    _fields_ = [
        ("hwnd", wintypes.HWND),
        ("message", wintypes.UINT),
        ("wParam", wintypes.WPARAM),
        ("lParam", wintypes.LPARAM),
        ("time", wintypes.DWORD),
        ("pt", wintypes.POINT),
    ]


def global_hotkey_enabled(qwidget_class):
    orig_nativeEvent = qwidget_class.nativeEvent

    def overrided_nativeEvent(self, eventType, message):
        if eventType == 'windows_generic_MSG':
            msg_ptr = ctypes.cast(int(message), ctypes.POINTER(MSG))
            msg = msg_ptr.contents
            if msg.message == win32con.WM_HOTKEY:
                hotkey_id = msg.wParam
                callback = self.__hotkey_id_to_callback.get(hotkey_id)
                if callback:
                    callback()
        return orig_nativeEvent(self, eventType, message)

    qwidget_class.nativeEvent = overrided_nativeEvent

    orig_init = qwidget_class.__init__

    def overrided_init(self, *args, **kwargs):
        self.__next_hotkey_id = 0
        self.__modifier_key_to_hotkey_id = {}
        self.__hotkey_id_to_callback = {}
        orig_init(self, *args, **kwargs)

    qwidget_class.__init__ = overrided_init

    def on_global_hotkey(self, sequence: str, callback: Callable[[], None]):
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

        hotkey_id = self.__modifier_key_to_hotkey_id.get((modifier, key))
        if hotkey_id:
            win32gui.UnregisterHotKey(self.winId(), hotkey_id)
        else:
            hotkey_id = self.__modifier_key_to_hotkey_id[(modifier, key)] = self.__next_hotkey_id
            self.__next_hotkey_id += 1

        self.__hotkey_id_to_callback[hotkey_id] = callback

        win32gui.RegisterHotKey(self.winId(), hotkey_id, modifier, key)

    qwidget_class.on_global_hotkey = on_global_hotkey

    return qwidget_class


if __name__ == '__main__':
    from PySide6.QtWidgets import QApplication, QWidget


    @global_hotkey_enabled
    class Widget(QWidget):

        def __init__(self):
            super().__init__()

            self.on_global_hotkey('ctrl alt e', self.toggle)

        def toggle(self):
            if self.isVisible():
                self.hide()
            else:
                self.show()


    app = QApplication([])

    widget = Widget()
    widget.show()

    app.exec()
