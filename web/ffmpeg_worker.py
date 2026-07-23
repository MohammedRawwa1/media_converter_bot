import subprocess


def _parse_out_time(timestr: str) -> float:
    # timestr like 00:01:23.456789
    try:
        parts = timestr.split(':')
        h = int(parts[0])
        m = int(parts[1])
        s = float(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        return 0.0


def get_duration(path: str) -> float:
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', path]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return float(out.strip())
    except Exception:
        return None


def convert_video(input_path: str, output_path: str, job_id: str, duration: float, progress_cb, finished_cb):
    # Basic x264/aac conversion with progress parsing via -progress pipe:1
    cmd = [
        'ffmpeg', '-y', '-i', input_path,
        '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-progress', 'pipe:1', '-nostats', output_path
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

    current_out_time = 0.0
    try:
        if proc.stdout is None:
            return
        for raw in proc.stdout:
            line = raw.strip()
            if not line:
                continue
            if '=' not in line:
                continue
            key, val = line.split('=', 1)
            if key == 'out_time':
                current_out_time = _parse_out_time(val)
                if duration and duration > 0:
                    pct = min(100.0, (current_out_time / duration) * 100.0)
                else:
                    pct = 0.0
                progress_cb(pct, f'encoding {pct:.1f}%')
            elif key == 'progress' and val == 'end':
                # finished signal from ffmpeg progress
                break

        proc.wait()
        if proc.returncode == 0:
            finished_cb(output_path)
        else:
            # attempt to read stderr
            err = proc.stderr.read() if proc.stderr else ''
            raise RuntimeError(f'ffmpeg failed: {err}')
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
        except Exception:
            pass
