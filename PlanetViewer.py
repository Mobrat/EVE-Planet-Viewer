#!/usr/bin/env python3
import csv
import bz2
import shutil
import threading
from pathlib import Path
import sqlite3
import requests
import tkinter as tk
from tkinter import ttk, messagebox
import os, sys

BASE_URL = "https://www.fuzzwork.co.uk/dump/latest/"
DATA_FILES = [
    "invTypes.csv",
    "mapDenormalize.csv",
    "mapSolarSystems.csv",
    "mapConstellations.csv",
    "mapRegions.csv",
]

def _resolve_storage_paths():
    base = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
    data_dir = base / "data"
    db_path = base / "eve_data.sqlite"
    try:
        data_dir.mkdir(exist_ok=True)
        return db_path, data_dir
    except PermissionError:
        local_base = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData/Local")) / "PlanetViewer"
        data_dir = local_base / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return local_base / "eve_data.sqlite", data_dir

DB_PATH, DATA_DIR = _resolve_storage_paths()

BATCH_SIZE = 10000
PLANET_BATCH_SIZE = 20000

PLANET_TYPES = [
    "Temperate", "Ice", "Gas", "Oceanic", "Lava",
    "Barren", "Storm", "Plasma", "Shattered", "Scorched Barren"
]
COL_HEADERS = [
    "Total Planets",
    "Planet (Temperate)",
    "Planet (Ice)",
    "Planet (Gas)",
    "Planet (Oceanic)",
    "Planet (Lava)",
    "Planet (Barren)",
    "Planet (Storm)",
    "Planet (Plasma)",
    "Planet (Shattered)",
    "Planet (Scorched Barren)",
    "Radius (km)",
]

def status_safe(ui, msg):
    ui.root.after(0, ui.status_var.set, msg)

def try_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def try_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def categorize_type(type_name):
    t = (type_name or "").lower()
    if "temperate" in t:
        return "Temperate"
    if "ice" in t:
        return "Ice"
    if "gas" in t:
        return "Gas"
    if "oceanic" in t:
        return "Oceanic"
    if "lava" in t:
        return "Lava"
    if "plasma" in t:
        return "Plasma"
    if "storm" in t:
        return "Storm"
    if "shattered" in t:
        return "Shattered"
    if "scorched" in t:
        return "Scorched Barren"
    if "barren" in t:
        return "Barren"
    return "Barren"

class EveDB:
    def __init__(self, path):
        self.path = path
        self.con = sqlite3.connect(self.path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.con.execute("PRAGMA journal_mode=WAL;")
            self.con.execute("PRAGMA synchronous=NORMAL;")
            self._ensure_schema()

    def _ensure_schema(self):
        cur = self.con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mapRegions (
                regionID INTEGER PRIMARY KEY,
                regionName TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mapConstellations (
                constellationID INTEGER PRIMARY KEY,
                regionID INTEGER,
                constellationName TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mapSolarSystems (
                solarSystemID INTEGER PRIMARY KEY,
                constellationID INTEGER,
                solarSystemName TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS invTypes (
                typeID INTEGER PRIMARY KEY,
                groupID INTEGER,
                typeName TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS planets (
                itemID INTEGER PRIMARY KEY,
                typeID INTEGER,
                groupID INTEGER,
                category TEXT,
                typeName TEXT,
                itemName TEXT,
                solarSystemID INTEGER,
                constellationID INTEGER,
                regionID INTEGER,
                orbitalID INTEGER,
                radius_km REAL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_region ON planets(regionID);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_constellation ON planets(constellationID);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_system ON planets(solarSystemID);")
        self.con.commit()

    def clear_all(self):
        with self.lock:
            cur = self.con.cursor()
            cur.execute("DELETE FROM planets;")
            cur.execute("DELETE FROM invTypes;")
            cur.execute("DELETE FROM mapSolarSystems;")
            cur.execute("DELETE FROM mapConstellations;")
            cur.execute("DELETE FROM mapRegions;")
            self.con.commit()

    def import_csv_invTypes(self, path):
        with self.lock, path.open("r", encoding="utf-8") as f, self.con:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                typeID = try_int(r.get("typeID"))
                groupID = try_int(r.get("groupID"))
                typeName = r.get("typeName")
                if typeID is None:
                    continue
                rows.append((typeID, groupID, typeName))
                if len(rows) >= BATCH_SIZE:
                    self.con.executemany(
                        "INSERT OR REPLACE INTO invTypes(typeID, groupID, typeName) VALUES (?,?,?)",
                        rows,
                    )
                    rows.clear()
            if rows:
                self.con.executemany(
                    "INSERT OR REPLACE INTO invTypes(typeID, groupID, typeName) VALUES (?,?,?)",
                    rows,
                )

    def import_csv_mapRegions(self, path):
        with self.lock, path.open("r", encoding="utf-8") as f, self.con:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                regionID = try_int(r.get("regionID"))
                name = r.get("regionName") or r.get("name")
                if regionID is None or not name:
                    continue
                rows.append((regionID, name))
                if len(rows) >= BATCH_SIZE:
                    self.con.executemany(
                        "INSERT OR REPLACE INTO mapRegions(regionID, regionName) VALUES (?,?)",
                        rows,
                    )
                    rows.clear()
            if rows:
                self.con.executemany(
                    "INSERT OR REPLACE INTO mapRegions(regionID, regionName) VALUES (?,?)",
                    rows,
                )

    def import_csv_mapConstellations(self, path):
        with self.lock, path.open("r", encoding="utf-8") as f, self.con:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                constellationID = try_int(r.get("constellationID"))
                regionID = try_int(r.get("regionID"))
                name = r.get("constellationName") or r.get("name")
                if constellationID is None or regionID is None:
                    continue
                rows.append((constellationID, regionID, name))
                if len(rows) >= BATCH_SIZE:
                    self.con.executemany(
                        "INSERT OR REPLACE INTO mapConstellations(constellationID, regionID, constellationName) VALUES (?,?,?)",
                        rows,
                    )
                    rows.clear()
            if rows:
                self.con.executemany(
                    "INSERT OR REPLACE INTO mapConstellations(constellationID, regionID, constellationName) VALUES (?,?,?)",
                    rows,
                )

    def import_csv_mapSolarSystems(self, path):
        with self.lock, path.open("r", encoding="utf-8") as f, self.con:
            reader = csv.DictReader(f)
            rows = []
            for r in reader:
                solarSystemID = try_int(r.get("solarSystemID"))
                constellationID = try_int(r.get("constellationID"))
                name = r.get("solarSystemName") or r.get("name")
                if solarSystemID is None or constellationID is None:
                    continue
                rows.append((solarSystemID, constellationID, name))
                if len(rows) >= BATCH_SIZE:
                    self.con.executemany(
                        "INSERT OR REPLACE INTO mapSolarSystems(solarSystemID, constellationID, solarSystemName) VALUES (?,?,?)",
                        rows,
                    )
                    rows.clear()
            if rows:
                self.con.executemany(
                    "INSERT OR REPLACE INTO mapSolarSystems(solarSystemID, constellationID, solarSystemName) VALUES (?,?,?)",
                    rows,
                )

    def build_planets_from_mapDenormalize(self, path):
        with self.lock:
            cur = self.con.cursor()
            type_map = {}
            for typeID, groupID, typeName in self.con.execute(
                "SELECT typeID, groupID, typeName FROM invTypes WHERE groupID = 7"
            ):
                type_map[typeID] = typeName
            orig_sync = cur.execute("PRAGMA synchronous").fetchone()[0]
            orig_temp = cur.execute("PRAGMA temp_store").fetchone()[0]
            orig_cache = cur.execute("PRAGMA cache_size").fetchone()[0]
            inserted = 0
            self.con.commit()
            cur.execute("PRAGMA synchronous=OFF")
            cur.execute("PRAGMA temp_store=MEMORY")
            cur.execute("PRAGMA cache_size=-200000")
            try:
                self.con.execute("BEGIN")
                cur.execute("DROP INDEX IF EXISTS idx_planets_region")
                cur.execute("DROP INDEX IF EXISTS idx_planets_constellation")
                cur.execute("DROP INDEX IF EXISTS idx_planets_system")
                with path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = []
                    for r in reader:
                        groupID = try_int(r.get("groupID"))
                        if groupID != 7:
                            continue
                        itemID = try_int(r.get("itemID"))
                        typeID = try_int(r.get("typeID"))
                        solarSystemID = try_int(r.get("solarSystemID"))
                        constellationID = try_int(r.get("constellationID"))
                        regionID = try_int(r.get("regionID"))
                        orbitalID = try_int(r.get("orbitalID")) if r.get("orbitalID") is not None else try_int(r.get("orbitID"))
                        radius_m = try_float(r.get("radius"))
                        radius_km = radius_m / 1000.0 if radius_m is not None else None
                        itemName = r.get("itemName")
                        if not (itemID and typeID and solarSystemID):
                            continue
                        typeName = type_map.get(typeID)
                        category = categorize_type(typeName)
                        rows.append((itemID, typeID, groupID, category, typeName, itemName,
                                     solarSystemID, constellationID, regionID, orbitalID, radius_km))
                        if len(rows) >= PLANET_BATCH_SIZE:
                            self.con.executemany(
                                "INSERT OR REPLACE INTO planets(itemID, typeID, groupID, category, typeName, itemName, solarSystemID, constellationID, regionID, orbitalID, radius_km) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                rows,
                            )
                            inserted += len(rows)
                            rows.clear()
                    if rows:
                        self.con.executemany(
                            "INSERT OR REPLACE INTO planets(itemID, typeID, groupID, category, typeName, itemName, solarSystemID, constellationID, regionID, orbitalID, radius_km) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                            rows,
                        )
                        inserted += len(rows)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_region ON planets(regionID)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_constellation ON planets(constellationID)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_planets_system ON planets(solarSystemID)")
                self.con.commit()
            except Exception:
                self.con.rollback()
                raise
            finally:
                self.con.commit()
                cur.execute(f"PRAGMA synchronous={orig_sync}")
                cur.execute(f"PRAGMA temp_store={orig_temp}")
                cur.execute(f"PRAGMA cache_size={orig_cache}")
            return inserted

    def list_regions(self):
        with self.lock:
            q = """
                SELECT regionID, regionName
                FROM mapRegions
                WHERE regionName IS NOT NULL
                AND TRIM(regionName) <> ''
                AND LOWER(regionName) <> 'no name'
                ORDER BY regionName COLLATE NOCASE
            """
            return list(self.con.execute(q))

    def constellations_in_region(self, region_id):
        with self.lock:
            q = """
                SELECT constellationID, constellationName
                FROM mapConstellations
                WHERE regionID = ?
                AND constellationName IS NOT NULL
                AND TRIM(constellationName) <> ''
                AND LOWER(constellationName) <> 'no name'
                ORDER BY constellationName COLLATE NOCASE
            """
            return list(self.con.execute(q, (region_id,)))

    def systems_in_constellation(self, constellation_id):
        with self.lock:
            q = """
                SELECT solarSystemID, solarSystemName
                FROM mapSolarSystems
                WHERE constellationID = ?
                AND solarSystemName IS NOT NULL
                AND TRIM(solarSystemName) <> ''
                AND LOWER(solarSystemName) <> 'no name'
                ORDER BY solarSystemName COLLATE NOCASE
            """
            return list(self.con.execute(q, (constellation_id,)))

    def region_name(self, region_id):
        with self.lock:
            row = self.con.execute("SELECT regionName FROM mapRegions WHERE regionID = ?", (region_id,)).fetchone()
            return row[0] if row else str(region_id)

    def constellation_name(self, constellation_id):
        with self.lock:
            row = self.con.execute("SELECT constellationName FROM mapConstellations WHERE constellationID = ?", (constellation_id,)).fetchone()
            return row[0] if row else str(constellation_id)

    def system_name(self, system_id):
        with self.lock:
            row = self.con.execute("SELECT solarSystemName FROM mapSolarSystems WHERE solarSystemID = ?", (system_id,)).fetchone()
            return row[0] if row else str(system_id)

    def count_planets_region(self, region_id):
        with self.lock:
            row = self.con.execute("SELECT COUNT(1) FROM planets WHERE regionID = ?", (region_id,)).fetchone()
            return row[0] if row else 0

    def count_planets_constellation(self, constellation_id):
        with self.lock:
            row = self.con.execute("SELECT COUNT(1) FROM planets WHERE constellationID = ?", (constellation_id,)).fetchone()
            return row[0] if row else 0

    def counts_by_category_system(self, system_id):
        with self.lock:
            q = "SELECT category, COUNT(1) FROM planets WHERE solarSystemID = ? GROUP BY category"
            counts = {k: 0 for k in PLANET_TYPES}
            for cat, c in self.con.execute(q, (system_id,)):
                if cat in counts:
                    counts[cat] = c
                else:
                    counts["Barren"] += c
            return counts

    def counts_by_category_region(self, region_id):
        with self.lock:
            q = "SELECT category, COUNT(1) FROM planets WHERE regionID = ? GROUP BY category"
            counts = {k: 0 for k in PLANET_TYPES}
            for cat, c in self.con.execute(q, (region_id,)):
                if cat in counts:
                    counts[cat] = c
                else:
                    counts["Barren"] += c
            return counts

    def counts_by_category_constellation(self, constellation_id):
        with self.lock:
            q = "SELECT category, COUNT(1) FROM planets WHERE constellationID = ? GROUP BY category"
            counts = {k: 0 for k in PLANET_TYPES}
            for cat, c in self.con.execute(q, (constellation_id,)):
                if cat in counts:
                    counts[cat] = c
                else:
                    counts["Barren"] += c
            return counts

    def planets_in_system(self, system_id):
        with self.lock:
            q = ("SELECT itemID, itemName, radius_km, typeName, category FROM planets WHERE solarSystemID = ? ORDER BY orbitalID")
            return list(self.con.execute(q, (system_id,)))

    def total_regions(self):
        with self.lock:
            q = """
                SELECT COUNT(*)
                FROM mapRegions
                WHERE regionName IS NOT NULL
                AND TRIM(regionName) <> ''
                AND LOWER(regionName) <> 'no name'
            """
            return self.con.execute(q).fetchone()[0]

    def total_constellations(self):
        with self.lock:
            q = """
                SELECT COUNT(*)
                FROM mapConstellations
                WHERE constellationName IS NOT NULL
                AND TRIM(constellationName) <> ''
                AND LOWER(constellationName) <> 'no name'
            """
            return self.con.execute(q).fetchone()[0]

    def total_systems(self):
        with self.lock:
            q = """
                SELECT COUNT(*)
                FROM mapSolarSystems
                WHERE solarSystemName IS NOT NULL
                AND TRIM(solarSystemName) <> ''
                AND LOWER(solarSystemName) <> 'no name'
            """
            return self.con.execute(q).fetchone()[0]

    def total_planets(self):
        with self.lock:
            q = """
                SELECT COUNT(*)
                FROM planets p
                JOIN mapRegions r ON r.regionID = p.regionID
                WHERE r.regionName IS NOT NULL
                AND TRIM(r.regionName) <> ''
                AND LOWER(r.regionName) <> 'no name'
            """
            return self.con.execute(q).fetchone()[0]

def download_file(url, dest):
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

def maybe_download_csv(local_dir, filename):
    csv_path = local_dir / filename
    bz2_path = local_dir / (filename + ".bz2")
    try:
        download_file(BASE_URL + filename + ".bz2", bz2_path)
        with bz2.open(bz2_path, "rb") as src, csv_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        return csv_path
    except requests.HTTPError:
        download_file(BASE_URL + filename, csv_path)
        return csv_path

class App:
    def __init__(self, root):
        import tkinter.font as tkfont
        self.root = root
        self.root.title("EVE Planet Viewer")
        self.db = EveDB(DB_PATH)
        root.columnconfigure(0, minsize=280)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(0, weight=1)
        self.sidebar = ttk.Frame(root)
        self.sidebar.grid(row=0, column=0, rowspan=2, sticky="nsw", padx=(8, 4), pady=8)
        self.counts_var = tk.StringVar(
            value="Regions: 0\nConstellations: 0\nSolar Systems: 0\nPlanets: 0"
        )
        ttk.Label(self.sidebar, textvariable=self.counts_var, justify="left", anchor="w").grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(self.sidebar, text="Search Regions").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(self.sidebar, textvariable=self.search_var)
        self.search_entry.grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(6, 0))
        self.sidebar.columnconfigure(1, weight=1)
        self.search_entry.bind("<KeyRelease>", self._on_search)
        self.search_entry.bind("<Return>", self._select_first_match)
        list_frame = ttk.Frame(self.sidebar)
        list_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(6, 0))
        list_frame.rowconfigure(0, weight=1)
        list_frame.columnconfigure(0, weight=1)
        self.sidebar.rowconfigure(2, weight=1)
        self.region_list = tk.Listbox(list_frame, activestyle="dotbox", exportselection=False)
        self.region_list.grid(row=0, column=0, sticky="nsew")
        lsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.region_list.yview)
        self.region_list.configure(yscrollcommand=lsb.set)
        lsb.grid(row=0, column=1, sticky="ns")
        self.region_list.bind("<<ListboxSelect>>", self._on_region_list_select)
        self.region_list.bind("<Double-Button-1>", self._on_region_list_select)
        self.region_list.bind("<Return>", self._on_region_list_select)
        mid = ttk.Frame(root)
        mid.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
        mid.rowconfigure(0, weight=1)
        mid.columnconfigure(0, weight=1)
        columns = COL_HEADERS
        self.tree = ttk.Treeview(mid, columns=columns, show="tree headings", selectmode="browse")
        self.tree.grid(row=0, column=0, sticky="nsew")
        style = ttk.Style(self.root)
        try:
            if style.theme_use() in ("winnative", "vista", "xpnative"):
                style.theme_use("clam")
        except Exception:
            pass
        try:
            style.layout("Treeview.Heading", [
                ("Treeheading.cell", {"sticky": "nswe", "children": [
                    ("Treeheading.border", {"sticky": "nswe", "children": [
                        ("Treeheading.padding", {"sticky": "nswe", "children": [
                            ("Treeheading.image", {"side": "left", "sticky": ""}),
                            ("Treeheading.text",  {"sticky": "nswe"})
                        ]})
                    ]})
                ]})
            ])
        except Exception:
            pass
        style.configure("Treeview", rowheight=22)
        base_font = tkfont.nametofont("TkDefaultFont")
        self._heading_font = tkfont.Font(family=base_font.cget("family"), size=9)
        style.configure("Treeview.Heading", padding=(6, 28), anchor="center", font=self._heading_font)
        self.tree.configure(style="Treeview")
        self._col_to_type = {f"Planet ({pt})": pt for pt in PLANET_TYPES}
        self.tree.heading("#0", text="Name")
        for col in columns:
            text = self._heading_text_for_column(col, 90)
            self.tree.heading(col, text=text, anchor="center")
        self._col_bases = {"#0": 225, **{col: 90 for col in columns}}
        self.tree.column("#0", width=self._col_bases["#0"], anchor="w", stretch=True)
        for col in columns:
            self.tree.column(col, width=self._col_bases[col], anchor="e", stretch=True)
        self._resizing = False
        self.root.after_idle(self._on_tree_configure)
        self.tree.bind("<Configure>", self._on_tree_configure)
        self.tree.tag_configure("evenrow", background="#ffffff")
        self.tree.tag_configure("oddrow",  background="#f4f6f8")
        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(mid, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.tree.bind("<<TreeviewOpen>>", self._on_open)
        self.tree.bind("<Button-1>", self._on_click)
        bottom = ttk.Frame(root)
        bottom.grid(row=1, column=1, sticky="ew", padx=(4, 8), pady=(0, 8))
        bottom.columnconfigure(0, weight=1)
        bottom.columnconfigure(1, weight=0)
        bottom.columnconfigure(2, weight=0)
        bottom.columnconfigure(3, weight=0)
        bottom.columnconfigure(4, weight=0)
        self.status_var = tk.StringVar(value="Ready.")
        self.status = ttk.Label(bottom, textvariable=self.status_var, anchor="w")
        self.status.grid(row=0, column=0, sticky="ew")
        self.show_region_types = tk.BooleanVar(value=False)
        self.show_const_types  = tk.BooleanVar(value=False)
        self.cb_region = ttk.Checkbutton(
            bottom, text="Region type totals", variable=self.show_region_types,
            command=self._on_toggle_breakdowns
        )
        self.cb_const = ttk.Checkbutton(
            bottom, text="Constellation type totals", variable=self.show_const_types,
            command=self._on_toggle_breakdowns
        )
        self.cb_region.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.cb_const.grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.fetch_btn = ttk.Button(bottom, text="Fetch Data", command=self.fetch_data)
        self.fetch_btn.grid(row=0, column=3, sticky="e", padx=(8, 0))
        self.clear_btn = ttk.Button(bottom, text="Clear", command=self.clear_view)
        self.clear_btn.grid(row=0, column=4, sticky="e", padx=(6, 0))
        self._all_regions = []
        self._filtered_regions = []
        self.refresh_regions()
        self.refresh_counts()
        self.search_entry.focus_set()

    def refresh_regions(self):
        self._all_regions = self.db.list_regions()
        self._apply_region_filter()

    def _apply_region_filter(self):
        q = (self.search_var.get() or "").lower()
        self._filtered_regions = [r for r in self._all_regions if q in (r[1] or "").lower()]
        self.region_list.delete(0, tk.END)
        for _rid, name in self._filtered_regions:
            self.region_list.insert(tk.END, name)

    def _on_search(self, event=None):
        self._apply_region_filter()
        self.search_entry.focus_set()

    def _select_first_match(self, event=None):
        if self._filtered_regions:
            self.region_list.selection_clear(0, tk.END)
            self.region_list.selection_set(0)
            self.region_list.activate(0)
            self._on_region_list_select()

    def _on_region_list_select(self, event=None):
        sel = self.region_list.curselection()
        if not sel:
            return
        idx = sel[0]
        region_id = self._filtered_regions[idx][0]
        self.populate_region(region_id, show_constellations=True)

    def _set_sidebar_state(self, state):
        self.search_entry.config(state=state)
        self.region_list.config(state=state)

    def fetch_data(self):
        self.region_list.delete(0, tk.END)
        self._all_regions.clear()
        self._filtered_regions.clear()
        self.fetch_btn.config(state="disabled")
        self._set_sidebar_state("disabled")
        self.root.update_idletasks()
        def enable_controls():
            self.fetch_btn.config(state="normal")
            self._set_sidebar_state("normal")
            self.search_entry.focus_set()
        def enable_and_refresh():
            enable_controls()
            self.refresh_regions()
            self.refresh_counts()
        def work():
            try:
                status_safe(self, "Downloading data…")
                local_paths = {}
                for fname in DATA_FILES:
                    status_safe(self, f"Downloading {fname}…")
                    local_paths[fname] = maybe_download_csv(DATA_DIR, fname)
                status_safe(self, "Importing into SQLite…")
                self.db.clear_all()
                self.db.import_csv_invTypes(local_paths["invTypes.csv"])
                self.db.import_csv_mapRegions(local_paths["mapRegions.csv"])
                self.db.import_csv_mapConstellations(local_paths["mapConstellations.csv"])
                self.db.import_csv_mapSolarSystems(local_paths["mapSolarSystems.csv"])
                status_safe(self, "Building planets table…")
                count = self.db.build_planets_from_mapDenormalize(local_paths["mapDenormalize.csv"])
                status_safe(self, f"Done. Imported {count:,} planets.")
            except Exception as e:
                status_safe(self, f"Error: {e}")
                self.root.after(0, messagebox.showerror, "Fetch Error", str(e))
            finally:
                self.root.after(0, enable_and_refresh)
        threading.Thread(target=work, daemon=True).start()

    def _iid(self, kind, id_):
        return f"{kind}:{id_}"

    def _parse_iid(self, iid, expected_prefix):
        if iid and iid.startswith(expected_prefix + ":"):
            try:
                return int(iid.split(":", 1)[1])
            except Exception:
                return 0
        return 0

    def _zebra_tag(self, parent):
        idx = len(self.tree.get_children(parent))
        return "evenrow" if idx % 2 == 0 else "oddrow"

    def populate_region(self, region_id, show_constellations=True):
        try:
            self.tree.delete(*self.tree.get_children(""))
        except Exception:
            for iid in self.tree.get_children(""):
                self.tree.delete(iid)
        self.current_region_id = region_id
        rname = self.db.region_name(region_id)
        if getattr(self, "show_region_types", None) and self.show_region_types.get():
            counts = self.db.counts_by_category_region(region_id)
            row = [sum(counts.values())] + [counts.get(t, 0) for t in PLANET_TYPES] + [""]
        else:
            total = self.db.count_planets_region(region_id)
            row = [total] + [""] * (len(COL_HEADERS) - 2) + [""]
        region_iid = self.tree.insert(
            "", "end",
            iid=self._iid("region", region_id),
            text=f"Region: {rname}",
            values=row,
            tags=("region",),
        )
        self.tree.item(region_iid, open=True)
        if show_constellations:
            self._load_constellations(region_iid, region_id)

    def _load_constellations(self, region_item, region_id):
        consts = self.db.constellations_in_region(region_id)
        for const_id, const_name in consts:
            if getattr(self, "show_const_types", None) and self.show_const_types.get():
                counts = self.db.counts_by_category_constellation(const_id)
                values = [sum(counts.values())] + [counts.get(t, 0) for t in PLANET_TYPES] + [""]
            else:
                total = self.db.count_planets_constellation(const_id)
                values = [total] + [""] * (len(COL_HEADERS) - 2) + [""]
            iid = self.tree.insert(
                region_item,
                "end",
                iid=self._iid("const", const_id),
                text=f"Constellation: {const_name}",
                values=values,
                tags=("constellation", self._zebra_tag(region_item)),
            )
            self.tree.insert(iid, "end", text="…", values=[""] * len(COL_HEADERS), tags=("placeholder",))

    def _load_systems(self, const_item, const_id):
        systems = self.db.systems_in_constellation(const_id)
        for sys_id, sys_name in systems:
            counts = self.db.counts_by_category_system(sys_id)
            row = [sum(counts.values())] + [counts.get(t, 0) for t in PLANET_TYPES] + [""]
            iid = self.tree.insert(
                const_item,
                "end",
                iid=self._iid("system", sys_id),
                text=f"System: {sys_name}",
                values=row,
                tags=("system", self._zebra_tag(const_item)),
            )
            self.tree.insert(iid, "end", text="…", values=[""] * len(COL_HEADERS), tags=("placeholder",))

    def _load_planets(self, sys_item, sys_id):
        planets = self.db.planets_in_system(sys_id)
        for itemID, itemName, radius_km, typeName, category in planets:
            kind = category or typeName or "Unknown"
            label = f"{itemName or '(Unnamed)'} — {kind}"
            radius_disp = f"{radius_km:,.0f}" if radius_km is not None else ""
            row = [""] * (len(COL_HEADERS) - 1) + [radius_disp]
            self.tree.insert(
                sys_item,
                "end",
                text=label,
                values=row,
                tags=("planet", self._zebra_tag(sys_item)),
            )

    def _wrap_type_heading(self, pt):
        s = pt.strip()
        if " " in s:
            a, b = s.split(" ", 1)
            return f"Planet\n({a}\n{b})"
        if len(s) > 8:
            cut = max(4, len(s)//2)
            return f"Planet\n({s[:cut]}-\n{s[cut:]})"
        return f"Planet\n({s})"

    def _heading_text_for_column(self, col, width_px):
        if col == "Total Planets":
            return "Total\nPlanets"
        if col == "Radius (km)":
            return "Radius\n(km)"
        pt = self._col_to_type.get(col)
        if not pt:
            return col
        pad = 10
        maxw = max(10, width_px - pad)
        one_line = f"({pt})"
        if self._heading_font.measure(one_line) <= maxw:
            return f"Planet\n{one_line}"
        part1, part2 = self._split_type_to_two_lines(pt, maxw)
        return f"Planet\n({part1}\n{part2})"

    def _split_type_to_two_lines(self, text, maxw):
        f = self._heading_font
        if " " in text:
            a, b = text.split(" ", 1)
            if f.measure(a) <= maxw and f.measure(b) <= maxw:
                return a, b
            long = a if f.measure(a) > f.measure(b) else b
            l1, l2 = self._hyphenate_to_fit(long, maxw)
            return (l1, l2) if long is a else (a, f"{l1}\n{l2}")
        return self._hyphenate_to_fit(text, maxw)

    def _hyphenate_to_fit(self, word, maxw):
        f = self._heading_font
        n = len(word)
        for offset in range(0, n):
            for cut in (n//2 - offset, n//2 + offset):
                if 2 <= cut < n-1:
                    left = word[:cut] + "-"
                    right = word[cut:]
                    if f.measure(left) <= maxw and f.measure(right) <= maxw:
                        return left, right
        cut = max(2, min(n-1, n//2))
        return word[:cut] + "-", word[cut:]

    def _on_tree_configure(self, event=None):
        if getattr(self, "_col_bases", None) is None:
            return
        if getattr(self, "_resizing", False):
            return
        self._resizing = True
        try:
            total_base = sum(self._col_bases.values())
            avail = max(100, self.tree.winfo_width())
            if total_base <= 0:
                return
            scale = avail / total_base
            for cid, base in self._col_bases.items():
                neww = max(60, int(base * scale))
                self.tree.column(cid, width=neww, stretch=True)
            for col in COL_HEADERS:
                cur_w = int(self.tree.column(col, option="width") or 90)
                new_text = self._heading_text_for_column(col, cur_w)
                self.tree.heading(col, text=new_text)
        finally:
            self._resizing = False

    def refresh_counts(self):
        try:
            r = self.db.total_regions()
            c = self.db.total_constellations()
            s = self.db.total_systems()
            p = self.db.total_planets()
        except Exception:
            r = c = s = p = 0
        self.counts_var.set(
            f"Regions: {r:,}\n"
            f"Constellations: {c:,}\n"
            f"Solar Systems: {s:,}\n"
            f"Planets: {p:,}"
        )

    def _on_open(self, event):
        item = self.tree.focus()
        if not item:
            return
        tags = self.tree.item(item, "tags") or ()
        children = self.tree.get_children(item)
        if "constellation" in tags:
            if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
                for c in children:
                    self.tree.delete(c)
                const_id = self._parse_iid(item, "const")
                self._load_systems(item, const_id)
        elif "system" in tags:
            if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
                for c in children:
                    self.tree.delete(c)
                sys_id = self._parse_iid(item, "system")
                self._load_planets(item, sys_id)

    def _on_click(self, event):
        if (event.state & 0x0001) == 0:
            return
        row_id = self.tree.identify_row(event.y)
        if not row_id:
            return
        tags = self.tree.item(row_id, "tags") or ()
        if "region" in tags:
            any_open = False
            for const_item in self.tree.get_children(row_id):
                if "constellation" in (self.tree.item(const_item, "tags") or ()):
                    if self.tree.item(const_item, "open"):
                        any_open = True
                        break
                    for sys_item in self.tree.get_children(const_item):
                        if ("system" in (self.tree.item(sys_item, "tags") or ())
                                and self.tree.item(sys_item, "open")):
                            any_open = True
                            break
                if any_open:
                    break
            if any_open:
                self._collapse_all_region(row_id)
            else:
                self._expand_all_region(row_id)
        elif "constellation" in tags:
            any_open = any(
                self.tree.item(s, "open")
                for s in self.tree.get_children(row_id)
                if "system" in (self.tree.item(s, "tags") or ())
            )
            if any_open:
                self._collapse_all_constellation(row_id)
            else:
                self._expand_all_constellation(row_id)
        elif "system" in tags:
            const_item = self.tree.parent(row_id)
            if not const_item:
                return
            sys_children = [
                s for s in self.tree.get_children(const_item)
                if "system" in (self.tree.item(s, "tags") or ())
            ]
            any_open = any(self.tree.item(s, "open") for s in sys_children)
            if any_open:
                self._collapse_systems_keep_constellation_open(const_item)
            else:
                self._expand_all_systems_in_constellation(const_item)

    def _expand_item(self, item):
        tags = self.tree.item(item, "tags") or ()
        children = self.tree.get_children(item)
        if "constellation" in tags:
            if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
                for c in children:
                    self.tree.delete(c)
                const_id = self._parse_iid(item, "const")
                self._load_systems(item, const_id)
        elif "system" in tags:
            if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
                for c in children:
                    self.tree.delete(c)
                sys_id = self._parse_iid(item, "system")
                self._load_planets(item, sys_id)

    def _expand_all_constellation(self, const_item):
        self.tree.item(const_item, open=True)
        children = self.tree.get_children(const_item)
        if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
            for c in children:
                self.tree.delete(c)
            const_id = self._parse_iid(const_item, "const")
            self._load_systems(const_item, const_id)
        for sys_item in self.tree.get_children(const_item):
            if "system" in (self.tree.item(sys_item, "tags") or ()): 
                self._expand_item(sys_item)
                self.tree.item(sys_item, open=True)

    def _collapse_all_constellation(self, const_item):
        for sys_item in self.tree.get_children(const_item):
            if "system" in (self.tree.item(sys_item, "tags") or ()): 
                self.tree.item(sys_item, open=False)
        self.tree.item(const_item, open=True)

    def _expand_all_region(self, region_item):
        self.tree.item(region_item, open=True)
        for const_item in self.tree.get_children(region_item):
            if "constellation" in (self.tree.item(const_item, "tags") or ()): 
                self.tree.item(const_item, open=True)
                children = self.tree.get_children(const_item)
                if children and "placeholder" in (self.tree.item(children[0], "tags") or ()): 
                    for c in children:
                        self.tree.delete(c)
                    const_id = self._parse_iid(const_item, "const")
                    self._load_systems(const_item, const_id)

    def _collapse_all_region(self, region_item):
        for const_item in self.tree.get_children(region_item):
            if "constellation" in (self.tree.item(const_item, "tags") or ()): 
                for sys_item in self.tree.get_children(const_item):
                    if "system" in (self.tree.item(sys_item, "tags") or ()): 
                        self.tree.item(sys_item, open=False)
                self.tree.item(const_item, open=False)
        self.tree.item(region_item, open=True)

    def _expand_all_systems_in_constellation(self, const_item):
        self.tree.item(const_item, open=True)
        for sys_item in self.tree.get_children(const_item):
            if "system" in (self.tree.item(sys_item, "tags") or ()): 
                self._expand_item(sys_item)
                self.tree.item(sys_item, open=True)

    def _collapse_systems_keep_constellation_open(self, const_item):
        for sys_item in self.tree.get_children(const_item):
            if "system" in (self.tree.item(sys_item, "tags") or ()): 
                self.tree.item(sys_item, open=False)
        self.tree.item(const_item, open=True)

    def _on_toggle_breakdowns(self):
        roots = self.tree.get_children("")
        if not roots:
            return
        region_item = roots[0]
        rid = self._parse_iid(region_item, "region")
        if not rid:
            return
        if self.show_region_types.get():
            rcounts = self.db.counts_by_category_region(rid)
            rrow = [sum(rcounts.values())] + [rcounts.get(t, 0) for t in PLANET_TYPES] + [""]
        else:
            rtotal = self.db.count_planets_region(rid)
            rrow = [rtotal] + [""] * (len(COL_HEADERS) - 2) + [""]
        for i, col in enumerate(COL_HEADERS):
            self.tree.set(region_item, col, rrow[i])
        for const_item in self.tree.get_children(region_item):
            tags = self.tree.item(const_item, "tags") or ()
            if "constellation" not in tags:
                continue
            cid = self._parse_iid(const_item, "const")
            if not cid:
                continue
            if self.show_const_types.get():
                ccounts = self.db.counts_by_category_constellation(cid)
                crow = [sum(ccounts.values())] + [ccounts.get(t, 0) for t in PLANET_TYPES] + [""]
            else:
                ctotal = self.db.count_planets_constellation(cid)
                crow = [ctotal] + [""] * (len(COL_HEADERS) - 2) + [""]
            for i, col in enumerate(COL_HEADERS):
                self.tree.set(const_item, col, crow[i])

    def clear_view(self):
        try:
            self.tree.delete(*self.tree.get_children(""))
        except Exception:
            for iid in self.tree.get_children(""):
                self.tree.delete(iid)
        self.status_var.set("Cleared.")
        self.current_region_id = None
        self.region_list.selection_clear(0, tk.END)


def main():
    root = tk.Tk()
    app = App(root)
    root.geometry("1600x720")
    root.mainloop()

if __name__ == "__main__":
    main()
