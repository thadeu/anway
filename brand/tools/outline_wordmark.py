"""Outlines the brand texts from Inter Tight into SVG path data, so that the
brand files do not need the font.

Run this only when a text changes. It needs fonttools and uharfbuzz:

    python3 -m venv .venv && .venv/bin/pip install fonttools uharfbuzz
    .venv/bin/python brand/tools/outline_wordmark.py InterTight[wght].ttf

Inter Tight is under the SIL Open Font License, from github.com/google/fonts.
"""

import json
import pathlib
import sys

import uharfbuzz as hb
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

TEXTS = {
    'wordmark': ('anway', 600, -0.04),
    'byline': ('by Clowk', 500, 0),
    'tagline': ('Background jobs for every language', 450, -0.01),
}


def outline(source, text, weight, tracking):
    font = instantiateVariableFont(TTFont(source), {'wght': weight})
    upem = font['head'].unitsPerEm
    glyphs = font.getGlyphSet()
    names = font.getGlyphOrder()

    shaper = hb.Font(hb.Face(hb.Blob.from_file_path(source)))
    shaper.set_variations({'wght': weight})

    buffer = hb.Buffer()
    buffer.add_str(text)
    buffer.guess_segment_properties()
    hb.shape(shaper, buffer, {'kern': True, 'liga': True})

    pen = SVGPathPen(glyphs)
    bounds = BoundsPen(glyphs)
    count = len(buffer.glyph_infos)
    x = 0.0

    for index, (info, position) in enumerate(zip(buffer.glyph_infos, buffer.glyph_positions)):
        transform = (1, 0, 0, -1, x + position.x_offset, -position.y_offset)
        glyph = glyphs[names[info.codepoint]]
        glyph.draw(TransformPen(pen, transform))
        glyph.draw(TransformPen(bounds, transform))
        x += position.x_advance

        if index < count - 1:
            x += tracking * upem

    return {
        'text': text,
        'font': f'Inter Tight {weight}, tracking {tracking}em',
        'units_per_em': upem,
        'advance': round(x, 2),
        'x_height': font['OS/2'].sxHeight,
        'cap_height': font['OS/2'].sCapHeight,
        'bounds': [round(value, 2) for value in bounds.bounds],
        'path': pen.getCommands(),
    }


source = sys.argv[1]
out = pathlib.Path(__file__).with_name('wordmark.json')
result = {name: outline(source, *spec) for name, spec in TEXTS.items()}

json.dump(result, out.open('w'), indent=1)

print(out, {name: round(entry['advance']) for name, entry in result.items()})
