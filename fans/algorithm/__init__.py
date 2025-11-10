import itertools


def sweep_line_overlaps(
        zones: list[any],
        *,
        get_beg=lambda zone: zone[0],
        get_end=lambda zone: zone[1],
) -> list[list[any]]:
    """
    Params:
        zones - list of intervals
    
    Returns:
        a list with same length as `zones`, where each item is overlapped zones with current zone
    """
    events = itertools.chain(*(
        [
            (get_beg(zone), True, i),
            (get_end(zone), False, i),
        ] for i, zone in enumerate(zones)
    ))
    events = sorted(events)
    active_zones = set()
    overlaps = [[] for _ in range(len(zones))]
    for _, is_beg, zone_index in events:
        if is_beg:
            for peer_zone_idx in active_zones:
                overlaps[peer_zone_idx].append(zone_index)
            overlaps[zone_index].extend(active_zones)
            active_zones.add(zone_index)
        elif zone_index in active_zones:
            active_zones.remove(zone_index)
    return [[zones[i] for i in idxs] for idxs in overlaps]
