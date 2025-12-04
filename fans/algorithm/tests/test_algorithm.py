from fans.algorithm import (
    sweep_line_overlaps,
)


def test_sweep_line_overlaps():
    assert sweep_line_overlaps([
        (0,          6),
        (  1,2        ),
        (        4,5  ),
                         (7,8),
    ]) == [
        ((0,6), [(1,2), (4,5)]),
    ]

    assert sweep_line_overlaps([
        (0,          6),
        (  1,2        ),
        (        4,5  ),
                         (7,8),
    ], mutual=True) == [
        ((0,6), [(1,2),(4,5)]),
        ((1,2), [(0,6)]),
        ((4,5), [(0,6)]),
    ]

    assert sweep_line_overlaps([
        (0,          6),
        (  1,2        ),
        (        4,5  ),
                         (7,8),
    ], mutual=True, non_empty_only=False) == [
        ((0,6), [(1,2),(4,5)]),
        ((1,2), [(0,6)]),
        ((4,5), [(0,6)]),
        ((7,8), []),
    ]

    assert sweep_line_overlaps([
        (0,          6),
        (  1,2        ),
        (        4,5  ),
                         (7,8),
    ], non_empty_only=False) == [
        ((0,6), [(1,2),(4,5)]),
        ((1,2), []),
        ((4,5), []),
        ((7,8), []),
    ]
