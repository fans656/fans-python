"""
Raw Input:
    https://docs.microsoft.com/en-us/windows/win32/inputdev/raw-input

To swallow key events:
    https://stackoverflow.com/questions/15042211/is-it-possible-to-swallow-a-key-in-raw-input
    https://www.codeproject.com/Articles/716591/Combining-Raw-Input-and-keyboard-Hook-to-selective
"""
import ctypes
import functools
from ctypes import windll, wintypes
user32 = windll.user32

import win32gui
import win32api
import win32con


def hook_keyboard(callback):
    wc = win32gui.WNDCLASS()
    wc.lpfnWndProc = functools.partial(_proc, callback)
    wc.lpszClassName = 'KeyListener'
    hinst = wc.hInstance = win32api.GetModuleHandle(None)
    classAtom = win32gui.RegisterClass(wc)
    hwnd = win32gui.CreateWindow(
        classAtom,
        'KeyListener',
        0,0,0,
        0, 0, # width, height
        0, 0,
        hinst, None
    )
    rid = RAWINPUTDEVICE()
    rid.usUsagePage = 1
    rid.usUsage = 6
    rid.dwFlags = RIDEV_INPUTSINK
    rid.hwndTarget = hwnd
    user32.RegisterRawInputDevices(ctypes.byref(rid), 1, ctypes.sizeof(RAWINPUTDEVICE))

    def run(allow_ctrl_c_to_quit=True, exit_code=0):
        if allow_ctrl_c_to_quit:
            import signal
            signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            win32gui.PumpMessages()
        except KeyboardInterrupt:
            return exit_code

    return run


def _proc(onkey, hwnd, msg, wparam, lparam):
    if msg == WM_INPUT:
        hRawInput = lparam
        raw_input = RAWINPUT()
        cbSize = wintypes.UINT(ctypes.sizeof(raw_input))
        user32.GetRawInputData(
            hRawInput,
            RID_INPUT,
            ctypes.byref(raw_input),
            ctypes.byref(cbSize),
            ctypes.sizeof(RAWINPUTHEADER),
        )
        event = RawKeyEvent(raw_input.keyboard)
        onkey(event)
    return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)


class RawKeyEvent:

    __fields__ = ['Key', 'KeyId', 'Message']

    def __init__(self, raw_input_keyboard):
        key = _to_key(raw_input_keyboard)
        self.Key = vk2name.get(key, chr(key))
        self.KeyId = key
        self.Message = raw_input_keyboard.Message

    @property
    def is_down(self):
        return self.Message == win32con.WM_KEYDOWN or self.Message == win32con.WM_SYSKEYDOWN

    def __repr__(self):
        return f"Key={self.Key}, KeyId={self.KeyId}, is_down={self.is_down}"


RIDEV_INPUTSINK = 0x100
WM_INPUT = 0xff
RID_INPUT = 0x10000003
MAPVK_VSC_TO_VK_EX = 3
RI_KEY_E0 = 2

msg2name = {v: k for k, v in win32con.__dict__.items() if k.startswith('WM_')}
vk2name = {v: k for k, v in win32con.__dict__.items() if k.startswith('VK_')}

VK_MENU = win32con.VK_MENU
VK_LMENU = win32con.VK_LMENU
VK_RMENU = win32con.VK_RMENU

VK_SHIFT = win32con.VK_SHIFT
VK_LSHIFT = win32con.VK_LSHIFT
VK_RSHIFT = win32con.VK_RSHIFT

VK_CONTROL = win32con.VK_CONTROL
VK_LCONTROL = win32con.VK_LCONTROL
VK_RCONTROL = win32con.VK_RCONTROL

class RAWINPUTDEVICE(ctypes.Structure):

    _fields_ = [
        ('usUsagePage', wintypes.USHORT),
        ('usUsage', wintypes.USHORT),
        ('dwFlags', wintypes.DWORD),
        ('hwndTarget', wintypes.HWND),
    ]

class RAWINPUTHEADER(ctypes.Structure):

    _fields_ = [
        ('dwType', wintypes.DWORD),
        ('dwSize', wintypes.DWORD),
        ('hDevice', wintypes.HANDLE),
        ('wParam', wintypes.WPARAM),
    ]

class RAWKEYBOARD(ctypes.Structure):

    _fields_ = [
        ('MakeCode', wintypes.USHORT),
        ('Flags', wintypes.USHORT),
        ('Reserved', wintypes.USHORT),
        ('VKey', wintypes.USHORT),
        ('Message', wintypes.UINT),
        ('ExtraInformation', wintypes.ULONG),
    ]

class RAWINPUT(ctypes.Structure):

    _fields_ = [
        ('header', RAWINPUTHEADER),
        ('keyboard', RAWKEYBOARD),
    ]

def _to_key(rk):
    # https://stackoverflow.com/a/18340130
    key = rk.VKey
    extended = rk.Flags & RI_KEY_E0
    if extended:
        if key == VK_CONTROL:
            key = VK_RCONTROL if extended else VK_LCONTROL
        if key == VK_MENU:
            key = VK_RMENU if extended else VK_LMENU
    else:
        key = win32api.MapVirtualKey(rk.MakeCode, MAPVK_VSC_TO_VK_EX) or key
    return key


if __name__ == '__main__':
    def func(event):
        print(event)

    run = hook_keyboard(func)
    run()
