import time
try: 
    from tqdm.auto import tqdm
except ImportError:
    import warnings
    warnings.warn("Tqdm not found, using generic progress bar -- worse display :P")
    tqdm = None

def _avg_speed_str(start, i):
    speed = (time.time() - start) / (i+1)
    if speed > 1:
        return f"{speed:.4f}s/it"
    else:
        return f"{1/speed:.2f}it/s"
    
def _interval(delta_sec):
    hours, mins, secs = 0, 0, int(delta_sec)
    if secs > 60:
        mins, secs = secs//60, secs%60
    if mins > 60:
        hours, mins = mins//60, mins%60
    return f"{hours:d}:{mins:02d}:{secs:02d}"

def _eta(start, i, total=None):
    since = time.time() - start
    if total is None:
        return _interval(since)

    remain = (total-i-1) * since / (i+1)
    return f"{_interval(since)}<{_interval(remain)}"

def _pbar(it, desc=None, total=None, disable=False, **kwargs):
    if total is None and hasattr(it, '__len__'):
        total = len(it)
    format = f"%{len(str(total)) if total else ''}d"
    if desc:
        format = f"{desc}: " + format
    if total:
        format = format + "/" + str(total)

    start = None
    try: 
        for i, e in enumerate(it):
            now = time.time()
            if start is None:
                start = now
                last_displayed = start
            yield e
            if not disable and (time.time() - last_displayed > 0.5 or i == 0):
                print('\r' + format%(i+1) + 
                      f" [{_eta(start, i, total)}, {_avg_speed_str(start, i)}]", 
                      end='')
                last_displayed = time.time()
    finally:
        print()

pbar = tqdm or _pbar