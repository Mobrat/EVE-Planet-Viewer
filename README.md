# EVE-Planet-Viewer
A simple tool made to view planets in EVE Online. Made with AI slop, but it does what I wanted it to do. Any real coders welcome to take the idea and run with it.
The purpose of the tool was to easily view planets and their assossiated type and radius to choose ideal PI locations.

How it works (and how to use it)

Click Fetch Data.

The app downloads these files from https://www.fuzzwork.co.uk/dump/latest/ (compressed when available) and builds a local database:

invTypes.csv
mapDenormalize.csv
mapSolarSystems.csv
mapConstellations.csv
mapRegions.csv

Progress appears in the status bar. When done, you'll see a “Done. Imported X planets.” message and the totals at the top-left will update.

Use the Search Regions box to filter the list.

Select a Region (single-click, Enter, or double-click) to populate the main tree.

Expand rows to navigate:

Click the chevron to open a Constellation → then a System → to see Planets.

Shift+click shortcuts:

On a Region row: expand/collapse all constellations (and their systems) under it.

On a Constellation row: expand/collapse all systems under it.

On a System row: toggle all sibling systems in that constellation.

Toggle Region type totals and Constellation type totals to show per-type counts (Temperate, Ice, Gas, etc.).

The Radius (km) column shows the planet radius where available.

Clear only resets the main view; it does not delete the database.

Re-run Fetch Data any time to refresh to the current public dump. (The database is rebuilt.) Should never actually need to perform this step unless CCP adds/removes/updates systems/planets.


Where data is stored

On first run (and each refresh), a SQLite DB and data folder are created automatically.

When frozen/bundled as an executable: next to the app binary (e.g., eve_data.sqlite, data/).

When run from source: next to the script file by default.

If the app cannot write to the above location, it falls back to:

Windows: %LOCALAPPDATA%/PlanetViewer (or ~/AppData/Local/PlanetViewer if LOCALAPPDATA is not set).


Files created:

eve_data.sqlite — the local database

data/ — working folder for downloaded CSVs

Resetting the DB: Close the app and delete eve_data.sqlite (and optionally data/). The next Fetch Data rebuilds it.
