import json, bisect

def load_lut(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _interp1(x_grid, y_grid, x):
    if x <= x_grid[0]: return y_grid[0]
    if x >= x_grid[-1]: return y_grid[-1]
    k = bisect.bisect_right(x_grid, x) - 1
    x0, x1 = x_grid[k], x_grid[k+1]
    y0, y1 = y_grid[k], y_grid[k+1]
    t = (x - x0) / (x1 - x0) if x1 != x0 else 0.0
    return y0 + t * (y1 - y0)

def _interp2(s_grid, r_grid, G, s, r):
    if s <= s_grid[0]: i0, i1, ts = 0, 0, 0.0
    elif s >= s_grid[-1]: i0, i1, ts = len(s_grid)-1, len(s_grid)-1, 0.0
    else:
        i0 = bisect.bisect_right(s_grid, s) - 1
        i1 = i0 + 1
        s0, s1 = s_grid[i0], s_grid[i1]
        ts = (s - s0) / (s1 - s0) if s1 != s0 else 0.0
    row0 = G[i0]; row1 = G[i1]
    g0 = _interp1(r_grid, row0, r)
    g1 = _interp1(r_grid, row1, r)
    return g0 + ts * (g1 - g0)

def size_from_lut(lut, s, b1, b2):
    L = min(b1, b2)
    if L <= 0: return 0.0
    r = b2 / b1 if b1 > 0 else 0.0
    g = _interp2(lut["s_grid"], lut["r_grid"], lut["g"], s, r)
    return L * max(0.0, g)
