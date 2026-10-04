# geo-discovery data

## `countries.json`

Country borders the service uses for country access (guest map scope, roaming
tolerance between neighbouring countries): `{ "<ISO 3166-1 alpha-2>": [polygon…] }`,
each polygon an outer ring followed by its holes, each ring `[[lng, lat], …]`.

- **Source:** Natural Earth 1:50m "Admin 0 – Countries" (public domain),
  <https://github.com/nvkelso/natural-earth-vector>.
- **Derived from** the iOS app's `Packages/Features/Maps/Sources/Maps/Resources/countries.json`
  (`core-platform-ios`, `Scripts/import-country-borders.py`: Douglas–Peucker at
  0.02°, 3 decimals), keeping only the code and the polygons. Using the very same
  borders as the app means the server and the device put a post in the same country.
- **Refresh:** re-run the iOS import script, then copy its codes and polygons here
  (`{code: polygons}`, compact JSON, sorted keys). Embedded at compile time.
