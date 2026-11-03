"""Builds the Anway brand SVG files from the mark geometry.

    python3 brand/tools/build.py && brand/tools/render.sh

The mark is one circle with three stations (app, queue, job) and two hops
between them. The queue sits on the top, and the app and the job are at mirror
angles. Every pair of parts has the same gap along the circle.
"""

import json
import math
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
SVG = ROOT / 'svg'
TEXTS = json.loads((ROOT / 'tools' / 'wordmark.json').read_text())

NAVY = '#0E1626'
INK = '#E7ECF5'
GREEN = '#3DDC97'
DEEP_GREEN = '#14A06B'
WHITE = '#FFFFFF'

ON_DARK = {'ink': INK, 'job': GREEN}
ON_LIGHT = {'ink': NAVY, 'job': DEEP_GREEN}

PRIMARY = {'radius': 19, 'start': 166, 'dots': (3.5, 4.5, 7), 'hops': (4, 5), 'gap': 3}
SMALL = {'radius': 21, 'start': 168, 'dots': (4.5, 5.5, 8), 'hops': (6, 6.5), 'gap': 3.5}


def parts(radius, start, dots, hops, gap):
    """Returns the parts in order. The hop lengths put the queue on the top
    and the job at the mirror angle of the app."""
    degrees = lambda length: math.degrees(length / radius)
    app, queue, job = dots
    first, second = hops

    first_length = 270 - start - degrees(app + gap + first / 2) - degrees(first / 2 + gap + queue)
    second_length = (540 - start) - 270 - degrees(queue + gap + second / 2) - degrees(second / 2 + gap + job)

    return [('dot', app, 'ink'), ('hop', first_length, first), ('dot', queue, 'ink'), ('hop', second_length, second), ('dot', job, 'job')]


def layout(spec):
    """Places the parts on the circle and centres their bounds in a 64 grid."""
    radius, gap = spec['radius'], spec['gap']
    angle, previous = spec['start'], None
    placed = []

    for part in parts(**spec):
        lead = part[1] if part[0] == 'dot' else part[2] / 2

        if previous is not None:
            angle += math.degrees((previous + gap + lead) / radius)

        if part[0] == 'dot':
            placed.append(('dot', angle, part[1], part[2]))
            previous = part[1]
        else:
            placed.append(('hop', angle, angle + part[1], part[2]))
            angle += part[1]
            previous = part[2] / 2

    point = lambda a: (radius * math.cos(math.radians(a)), radius * math.sin(math.radians(a)))
    corners = []

    for shape in placed:
        if shape[0] == 'dot':
            x, y = point(shape[1])
            corners += [(x - shape[2], y - shape[2]), (x + shape[2], y + shape[2])]
        else:
            for step in range(61):
                x, y = point(shape[1] + (shape[2] - shape[1]) * step / 60)
                corners += [(x - shape[3] / 2, y - shape[3] / 2), (x + shape[3] / 2, y + shape[3] / 2)]

    left, right = min(c[0] for c in corners), max(c[0] for c in corners)
    top, bottom = min(c[1] for c in corners), max(c[1] for c in corners)
    cx, cy = 32 - (left + right) / 2, 32 - (top + bottom) / 2
    box = (32 - (right - left) / 2, 32 - (bottom - top) / 2, right - left, bottom - top)

    return {'shapes': placed, 'center': (cx, cy), 'radius': radius, 'box': box}


def mark_body(geometry, colors=None, classes=False):
    """SVG elements for the mark. With classes, colors come from a style."""
    cx, cy = geometry['center']
    radius = geometry['radius']
    point = lambda a: (cx + radius * math.cos(math.radians(a)), cy + radius * math.sin(math.radians(a)))
    out = []

    for shape in geometry['shapes']:
        if shape[0] == 'dot':
            x, y = point(shape[1])
            role = shape[3]
            paint = f'class="{role}"' if classes else f'fill="{colors[role]}"'
            out.append(f'<circle cx="{x:.3f}" cy="{y:.3f}" r="{shape[2]}" {paint}/>')
        else:
            (x1, y1), (x2, y2) = point(shape[1]), point(shape[2])
            paint = 'class="hop"' if classes else f'stroke="{colors["ink"]}"'
            out.append(f'<path d="M{x1:.3f} {y1:.3f}A{radius} {radius} 0 0 1 {x2:.3f} {y2:.3f}" fill="none" {paint} stroke-width="{shape[3]}" stroke-linecap="round"/>')

    return ''.join(out)


def svg(view_box, body, title='Anway'):
    return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}" role="img" aria-label="{title}"><title>{title}</title>{body}</svg>\n'


def box_string(box):
    return ' '.join(f'{value:.3f}' for value in box)


def text_group(name, x, baseline, x_height, color):
    """Places an outlined text so that its x-height is x_height units."""
    entry = TEXTS[name]
    scale = x_height / entry['x_height']

    return f'<path transform="translate({x:.3f} {baseline:.3f}) scale({scale:.6f})" d="{entry["path"]}" fill="{color}"/>', scale


def ink(name, scale):
    left, top, right, bottom = TEXTS[name]['bounds']

    return left * scale, top * scale, right * scale, bottom * scale


def placed_mark(geometry, colors, x, y, height):
    bx, by, bw, bh = geometry['box']
    scale = height / bh

    return f'<g transform="translate({x:.3f} {y:.3f}) scale({scale:.6f}) translate({-bx:.3f} {-by:.3f})">{mark_body(geometry, colors)}</g>', bw * scale


def lockup(primary, colors, word_color, byline=None):
    """Horizontal lockup. The mark is 1.3 x-heights tall and sits on the centre
    of the x-height. The gap is 0.55 x-heights."""
    x_height = 100
    mark_height = 1.3 * x_height
    baseline = x_height / 2 + mark_height / 2

    mark, mark_width = placed_mark(primary, colors, 0, 0, mark_height)
    _, scale = text_group('wordmark', 0, 0, x_height, word_color)
    left, top, right, bottom = ink('wordmark', scale)
    word_x = mark_width + 0.55 * x_height - left
    word, _ = text_group('wordmark', word_x, baseline, x_height, word_color)

    width = word_x + right
    top_edge = min(0, baseline + top)
    bottom_edge = max(mark_height, baseline + bottom)
    body = mark + word

    if byline:
        rule_x = width + 0.6 * x_height
        by_height = 0.42 * x_height
        by_group, by_scale = text_group('byline', 0, 0, by_height, byline['text'])
        by_left, by_top, by_right, by_bottom = ink('byline', by_scale)
        by_x = rule_x + 0.45 * x_height - by_left
        body += f'<rect x="{rule_x:.3f}" y="{baseline - 1.05 * x_height:.3f}" width="2" height="{1.1 * x_height:.3f}" fill="{byline["rule"]}"/>'
        body += text_group('byline', by_x, baseline, by_height, byline['text'])[0]
        width = by_x + by_right

    return svg(f'0 {top_edge:.3f} {width:.3f} {bottom_edge - top_edge:.3f}', body, 'Anway')


def stacked(primary, colors, word_color):
    """Stacked lockup. The mark is 2.6 x-heights tall, with 0.8 x-heights of
    space above the word."""
    x_height = 100
    mark_height = 2.6 * x_height

    _, scale = text_group('wordmark', 0, 0, x_height, word_color)
    left, top, right, bottom = ink('wordmark', scale)
    word_width = right - left

    bx, by, bw, bh = primary['box']
    mark_width = bw * mark_height / bh
    width = max(word_width, mark_width)

    mark, _ = placed_mark(primary, colors, (width - mark_width) / 2, 0, mark_height)
    baseline = mark_height + 0.8 * x_height - top
    word, _ = text_group('wordmark', (width - word_width) / 2 - left, baseline, x_height, word_color)

    return svg(f'0 0 {width:.3f} {baseline + bottom:.3f}', mark + word, 'Anway')


def wordmark(color):
    entry = TEXTS['wordmark']
    left, top, right, bottom = entry['bounds']

    return svg(f'{left} {top} {right - left} {bottom - top}', f'<path d="{entry["path"]}" fill="{color}"/>', 'Anway')


def squircle(size=100, power=5, steps=180):
    points = []

    for step in range(steps):
        t = 2 * math.pi * step / steps
        c, s = math.cos(t), math.sin(t)
        x = size / 2 + size / 2 * math.copysign(abs(c) ** (2 / power), c)
        y = size / 2 + size / 2 * math.copysign(abs(s) ** (2 / power), s)
        points.append(f'{x:.3f} {y:.3f}')

    return 'M' + 'L'.join(points) + 'Z'


TILES = {
    'navy': {'top': '#1A2943', 'bottom': '#0C1424', 'colors': {'ink': '#EEF2F8', 'job': GREEN}, 'edge': (WHITE, 0.08)},
    'green': {'top': '#52E5A5', 'bottom': '#2FC888', 'colors': {'ink': NAVY, 'job': WHITE}, 'edge': None},
    'light': {'top': WHITE, 'bottom': '#E9EDF3', 'colors': {'ink': NAVY, 'job': DEEP_GREEN}, 'edge': (NAVY, 0.08)},
}


def app_icon(primary, tile, shape):
    """An app icon. The squircle shape is for documents and stores. The square
    shape is for platforms that cut their own mask, such as iOS and Android.
    The mark stays inside the maskable safe zone in both shapes."""
    outline = squircle() if shape == 'squircle' else 'M0 0H100V100H0Z'
    defs = f'<defs><linearGradient id="tile" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{tile["top"]}"/><stop offset="1" stop-color="{tile["bottom"]}"/></linearGradient></defs>'
    body = defs + f'<path d="{outline}" fill="url(#tile)"/>'

    if tile['edge'] and shape == 'squircle':
        color, opacity = tile['edge']
        body += f'<path d="{outline}" fill="none" stroke="{color}" stroke-opacity="{opacity}" stroke-width=".6" transform="translate(.3 .3) scale(.994)"/>'

    body += f'<svg x="15" y="16" width="70" height="70" viewBox="0 0 64 64">{mark_body(primary, tile["colors"])}</svg>'

    return svg('0 0 100 100', body, 'Anway')


def favicon(small):
    """The favicon follows the light or dark theme of the browser."""
    style = (
        f'<style>.ink{{fill:{NAVY}}}.hop{{stroke:{NAVY}}}.job{{fill:{DEEP_GREEN}}}'
        f'@media (prefers-color-scheme:dark){{.ink{{fill:{INK}}}.hop{{stroke:{INK}}}.job{{fill:{GREEN}}}}}</style>'
    )
    side = max(small['box'][2], small['box'][3]) + 3

    return svg(f'{32 - side / 2:.3f} {32 - side / 2:.3f} {side:.3f} {side:.3f}', style + mark_body(small, classes=True), 'Anway')


def favicon_tile(small):
    """A favicon on a navy tile, for PNG and ICO files that cannot follow the
    browser theme."""
    side = max(small['box'][2], small['box'][3]) + 3
    inset = side * 0.06
    full = side + 2 * inset
    x0 = 32 - side / 2 - inset
    body = f'<rect x="{x0:.3f}" y="{x0:.3f}" width="{full:.3f}" height="{full:.3f}" rx="{full * 0.22:.3f}" fill="{NAVY}"/>' + mark_body(small, ON_DARK)

    return svg(f'{x0:.3f} {x0:.3f} {full:.3f} {full:.3f}', body, 'Anway')


def social(primary):
    """The GitHub social preview, 1280 by 640."""
    x_height = 64
    mark_height = 1.3 * x_height
    _, scale = text_group('wordmark', 0, 0, x_height, INK)
    left, top, right, bottom = ink('wordmark', scale)

    bx, by, bw, bh = primary['box']
    mark_width = bw * mark_height / bh
    total = mark_width + 0.55 * x_height + (right - left)
    x = (1280 - total) / 2
    baseline = 310

    mark, _ = placed_mark(primary, ON_DARK, x, baseline - x_height / 2 - mark_height / 2, mark_height)
    word, _ = text_group('wordmark', x + mark_width + 0.55 * x_height - left, baseline, x_height, INK)

    tag_height = 19
    _, tag_scale = text_group('tagline', 0, 0, tag_height, '#9AA8BF')
    tag_left, _, tag_right, _ = ink('tagline', tag_scale)
    tagline, _ = text_group('tagline', (1280 - (tag_right - tag_left)) / 2 - tag_left, baseline + 118, tag_height, '#9AA8BF')

    defs = '<defs><radialGradient id="glow" cx=".5" cy=".42" r=".6"><stop offset="0" stop-color="#16233B"/><stop offset="1" stop-color="#0A101C"/></radialGradient></defs>'

    return svg('0 0 1280 640', defs + '<rect width="1280" height="640" fill="url(#glow)"/>' + mark + word + tagline, 'Anway')


def main():
    SVG.mkdir(exist_ok=True)

    primary = layout(PRIMARY)
    small = layout(SMALL)
    small_side = max(small['box'][2], small['box'][3]) + 3
    small_box = f'{32 - small_side / 2:.3f} {32 - small_side / 2:.3f} {small_side:.3f} {small_side:.3f}'

    files = {
        'mark-on-dark.svg': svg('0 0 64 64', mark_body(primary, ON_DARK)),
        'mark-on-light.svg': svg('0 0 64 64', mark_body(primary, ON_LIGHT)),
        'mark-white.svg': svg('0 0 64 64', mark_body(primary, {'ink': WHITE, 'job': WHITE})),
        'mark-navy.svg': svg('0 0 64 64', mark_body(primary, {'ink': NAVY, 'job': NAVY})),
        'mark-small-on-dark.svg': svg(small_box, mark_body(small, ON_DARK)),
        'mark-small-on-light.svg': svg(small_box, mark_body(small, ON_LIGHT)),
        'favicon.svg': favicon(small),
        'favicon-tile.svg': favicon_tile(small),
        'wordmark-on-dark.svg': wordmark(INK),
        'wordmark-on-light.svg': wordmark(NAVY),
        'lockup-on-dark.svg': lockup(primary, ON_DARK, INK),
        'lockup-on-light.svg': lockup(primary, ON_LIGHT, NAVY),
        'lockup-stacked-on-dark.svg': stacked(primary, ON_DARK, INK),
        'lockup-stacked-on-light.svg': stacked(primary, ON_LIGHT, NAVY),
        'lockup-clowk-on-dark.svg': lockup(primary, ON_DARK, INK, {'text': '#9AA8BF', 'rule': '#34466A'}),
        'lockup-clowk-on-light.svg': lockup(primary, ON_LIGHT, NAVY, {'text': '#5B6B85', 'rule': '#C6CEDB'}),
        'social-preview.svg': social(primary),
    }

    for name, tile in TILES.items():
        files[f'app-icon-{name}.svg'] = app_icon(primary, tile, 'squircle')

    files['app-icon-square.svg'] = app_icon(primary, TILES['navy'], 'square')

    for name, content in files.items():
        (SVG / name).write_text(content)

    print(f'{len(files)} files in {SVG}')


if __name__ == '__main__':
    main()
