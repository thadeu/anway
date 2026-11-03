"""Writes brand/index.html, a page that shows every PNG in brand/png.

    python3 brand/tools/gallery.py

render.sh runs it after it renders the files. It uses only the standard
library, and the page has no external files.
"""

import html
import pathlib
import struct

ROOT = pathlib.Path(__file__).resolve().parent.parent
PNG = ROOT / 'png'

GROUPS = [
    ('Mark', 'The mark alone. Use the small drawing at 24 px and below.', ('mark-',)),
    ('Lockups', 'The mark with the wordmark. Keep clear space equal to the x-height.', ('lockup-', 'wordmark-')),
    ('App icons', 'Squircle tiles for documents and stores.', ('app-icon-',)),
    ('Platform icons', 'Square tiles. iOS, Android, and PWA cut their own mask.', ('icon-', 'apple-touch-icon')),
    ('Favicons', 'On a navy tile, for places that cannot use favicon.svg.', ('favicon-',)),
    ('Social', 'The GitHub social preview.', ('social-',)),
]


def size(path):
    with path.open('rb') as file:
        header = file.read(24)

    return struct.unpack('>II', header[16:24])


def surface(name):
    if 'on-light' in name or 'navy' in name and name.startswith('mark-'):
        return 'light'

    if 'on-dark' in name or 'white' in name:
        return 'dark'

    return 'check'


def kilobytes(path):
    return f'{path.stat().st_size / 1024:.0f} KB'


def card(path):
    width, height = size(path)
    name = html.escape(path.name)
    href = f'png/{name}'

    return f'''
      <figure class="card">
        <a class="frame {surface(path.name)}" href="{href}" title="Open {name}">
          <img src="{href}" alt="{name}" loading="lazy">
        </a>
        <figcaption>
          <a href="{href}" download>{name}</a>
          <span>{width} × {height} · {kilobytes(path)}</span>
        </figcaption>
      </figure>'''


def main():
    files = sorted(PNG.glob('*.png'))
    used = set()
    sections = []

    for title, note, prefixes in GROUPS:
        group = [path for path in files if path.name.startswith(prefixes) and path not in used]
        used.update(group)

        if group:
            cards = ''.join(card(path) for path in group)
            sections.append(f'<section><h2>{title}</h2><p class="note">{note}</p><div class="grid">{cards}</div></section>')

    rest = [path for path in files if path not in used]

    if rest:
        sections.append(f'<section><h2>Other</h2><div class="grid">{"".join(card(path) for path in rest)}</div></section>')

    page = f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Anway brand files</title>
<link rel="icon" href="svg/favicon.svg" type="image/svg+xml">
<style>
:root{{--bg:#0A101C;--panel:#101A2C;--line:#1E2B42;--text:#E7ECF5;--sub:#9AA8BF;--green:#3DDC97}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 Inter,system-ui,-apple-system,sans-serif}}
main{{max-width:1200px;margin:0 auto;padding:48px 16px 80px}}
header{{display:flex;flex-wrap:wrap;align-items:flex-end;justify-content:space-between;gap:16px;margin-bottom:40px}}
header img{{display:block;height:44px;width:auto}}
header p{{margin:12px 0 0;color:var(--sub);max-width:620px}}
header a{{color:var(--green)}}
h2{{margin:40px 0 4px;font-size:13px;font-weight:500;letter-spacing:.06em;text-transform:uppercase;color:var(--sub);font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}
.note{{margin:0 0 16px;color:var(--sub);font-size:14px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:16px}}
.card{{margin:0;background:var(--panel);border:1px solid var(--line);border-radius:2px;overflow:hidden}}
.frame{{display:flex;align-items:center;justify-content:center;height:180px;padding:20px}}
.frame img{{max-width:100%;max-height:100%;object-fit:contain}}
.frame.dark{{background:#0E1626}}
.frame.light{{background:#F4F6FA}}
.frame.check{{background-color:#C9D0DB;background-image:linear-gradient(45deg,#B7C0CD 25%,transparent 25%,transparent 75%,#B7C0CD 75%),linear-gradient(45deg,#B7C0CD 25%,transparent 25%,transparent 75%,#B7C0CD 75%);background-size:16px 16px;background-position:0 0,8px 8px}}
figcaption{{display:flex;flex-direction:column;gap:2px;padding:10px 12px;border-top:1px solid var(--line)}}
figcaption a{{color:var(--text);text-decoration:none;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;overflow-wrap:anywhere}}
figcaption a:hover{{color:var(--green)}}
figcaption span{{color:var(--sub);font-size:12px}}
</style>
</head>
<body>
<main>
  <header>
    <div>
      <img src="png/lockup-on-dark.png" alt="Anway">
      <p>{len(files)} PNG files. Click a name to download it. The SVG sources are in <code>svg/</code>, and the rules are in <a href="README.md">README.md</a>.</p>
    </div>
  </header>
  {"".join(sections)}
</main>
</body>
</html>
'''

    (ROOT / 'index.html').write_text(page)
    print(f'{ROOT / "index.html"}: {len(files)} files')


if __name__ == '__main__':
    main()
