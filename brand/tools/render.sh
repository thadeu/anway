#!/usr/bin/env bash
# Renders the brand PNG and ICO files from svg/. Needs rsvg-convert (librsvg)
# and magick (ImageMagick).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
svg="$root/svg"
png="$root/png"

mkdir -p "$png"
rm -f "$png"/*.png "$root/favicon.ico"

render() {
  rsvg-convert -w "$2" "$svg/$1.svg" -o "$png/$3.png"
}

for size in 64 128 256 512 1024; do
  render mark-on-dark "$size" "mark-on-dark-$size"
  render mark-on-light "$size" "mark-on-light-$size"
done

render mark-white 512 mark-white-512
render mark-navy 512 mark-navy-512

for name in navy green light; do
  render "app-icon-$name" 1024 "app-icon-$name-1024"
  render "app-icon-$name" 512 "app-icon-$name-512"
done

render app-icon-square 180 apple-touch-icon
render app-icon-square 192 icon-192
render app-icon-square 512 icon-512
render app-icon-square 1024 icon-1024

for size in 16 32 48; do
  render favicon-tile "$size" "favicon-$size"
done

magick "$png/favicon-16.png" "$png/favicon-32.png" "$png/favicon-48.png" "$root/favicon.ico"

for name in lockup-on-dark lockup-on-light lockup-stacked-on-dark lockup-stacked-on-light lockup-clowk-on-dark lockup-clowk-on-light wordmark-on-dark wordmark-on-light; do
  render "$name" 1600 "$name"
done

render social-preview 1280 social-preview

echo "$(ls "$png" | wc -l | tr -d ' ') PNG files in $png"

python3 "$root/tools/gallery.py"
