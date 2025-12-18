import itertools


def sweep_line_overlaps(
        intervals: list[any],
        *,
        get_beg=lambda interval: interval[0],
        get_end=lambda interval: interval[1],
        mutual: bool = False,
        non_empty_only: bool = True,
) -> list[list[any]]:
    """
    Find overlapped intervals using sweep line algorithm.

    Params:
        intervals - list of intervals
        get_beg - callable to get interval begin
        get_end - callable to get interval end
        mutual - when interval_1 (first met) and interval_2 (second met) overlap,
            whether add interval_1 to overlaps of interval_2

    Returns:
        A list of (interval, overlapped_intervals) tuples
        where `interval` is the original interval given
    """
    # events of (pos, is_begin, index) from all intervals
    events = itertools.chain(*(
        [
            (get_beg(interval), True, i),
            (get_end(interval), False, i),
        ] for i, interval in enumerate(intervals)
    ))
    events = sorted(events)

    active_intervals = set()
    overlaps = [[] for _ in range(len(intervals))]
    for _, is_beg, interval_index in events:
        if is_beg:
            for peer_interval_idx in active_intervals:
                overlaps[peer_interval_idx].append(interval_index)
            if mutual:
                overlaps[interval_index].extend(active_intervals)
            active_intervals.add(interval_index)
        elif interval_index in active_intervals:
            active_intervals.remove(interval_index)

    return [
        (intervals[i_interval], [intervals[i] for i in idxs])
        for i_interval, idxs in enumerate(overlaps)
        if not non_empty_only or idxs
    ]
