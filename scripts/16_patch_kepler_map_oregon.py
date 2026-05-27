import re
import sys
import os


# ---------------------------------------------------------------------------
# When you save the html from kepler.gl, it defaults to centering on the bay
# area. This script patches the HTML to change the center and zoom to Oregon.
# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_HTML = os.path.expanduser("kepler.gl.html")  # default kepler export name
OUTPUT_HTML = os.path.expanduser("oregon_map.html")

# Oregon center + zoom
TARGET_LAT = 44.0
TARGET_LNG = -120.5
TARGET_ZOOM = 7

# ---------------------------------------------------------------------------
# PATCH
# ---------------------------------------------------------------------------

print(f"Reading from: {INPUT_HTML}")
print(f"File size: {os.path.getsize(INPUT_HTML) / 1024 / 1024:.1f} MB")

# Read in chunks to avoid crashing on large files
chunk_size = 10 * 1024 * 1024  # 10MB chunks

found = False
buffer = ""

with (
    open(INPUT_HTML, "r", encoding="utf-8", errors="replace") as f_in,
    open(OUTPUT_HTML, "w", encoding="utf-8") as f_out,
):
    while True:
        chunk = f_in.read(chunk_size)
        if not chunk:
            # Write any remaining buffer
            f_out.write(buffer)
            break

        # Combine with leftover from previous chunk to avoid splitting the pattern
        data = buffer + chunk

        # Patch mapState
        new_data, n = re.subn(
            r'"mapState"\s*:\s*\{[^}]*\}',
            f'"mapState":{{"pitch":0,"bearing":0,"latitude":{TARGET_LAT},"longitude":{TARGET_LNG},"zoom":{TARGET_ZOOM}}}',
            data,
        )

        if n > 0 and not found:
            print(f"  ✓ Patched mapState → lat:{TARGET_LAT}, lng:{TARGET_LNG}, zoom:{TARGET_ZOOM}")
            found = True

        # Keep the last 500 chars as buffer in case the pattern spans a chunk boundary
        f_out.write(new_data[:-500])
        buffer = new_data[-500:]

if not found:
    print("  ⚠️  mapState pattern not found — check the HTML structure")
    print("  Try searching for 'mapState' manually in a small portion of the file:")
    print("  head -c 100000 your_file.html | grep -o 'mapState[^}]*}'")
else:
    print(f"\nSaved to: {OUTPUT_HTML}")
    print(f"File size: {os.path.getsize(OUTPUT_HTML) / 1024 / 1024:.1f} MB")
    print("\nOpen oregon_map.html in your browser — it should open centered on Oregon.")
