from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from pybaseball import playerid_lookup, cache
# from pybaseball import batting_stats, pitching_stats
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
import pandas as pd
import os
import datetime
import time
import sys
import os

app = Flask(__name__, static_folder="static")
CORS(app, resources={
    r"/*": {
        "origins": [ "https://schipperstatlines.onrender.com", "https://website-a7a.pages.dev", "http://127.0.0.1:5501"],
    }
})
# "http://127.0.0.1:5501",
# CORS(app resources={r"/*": {"origins": "website-a7a.pages.dev"}})
cache.enable()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "baseball.db")




@app.route("/")
def serve_index():
    return send_from_directory("static", "index.html")

@app.route("/team")
def get_team_stats():
    """Unified endpoint that returns both batting and pitching stats"""
    try:
        team = request.args.get("team", "").strip()
        mode = request.args.get("mode", "season").lower()

        if not team:
            return jsonify({"error": "Enter team"}), 400

        team_id, year = parse_team_input(team)

        # Get combined stats
        return handle_combined_team_stats(team_id, year, mode)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


def handle_combined_team_stats(team_id, year, mode):
    """Get both batting and pitching stats in one query - updated for StatHead format"""
    try:
        conn = sqlite3.connect(DB_PATH)

        # Track the actual year being used
        actual_year = None

        if mode == "season":
            actual_year = year or 2024
            query = """
            SELECT yearid, teamid, 
                   -- Basic team stats
                   g, w, l, r, ra,
                   -- We'll calculate playoff stats separately
                   0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
            FROM lahman_teams 
            WHERE teamid = ? AND yearid = ?
            """
            df = pd.read_sql_query(query, conn, params=(team_id, actual_year))

        elif mode in ["franchise", "career", "overall"]:

            # Check for franchise moves - Milwaukee Brewers example
            franchise_ids = get_franchise_team_ids(team_id)

            if len(franchise_ids) > 1:
                # Multiple team IDs for this franchise
                placeholders = ",".join(["?" for _ in franchise_ids])
                query = f"""
                SELECT 'FRANCHISE' as teamid, 
                       COUNT(*) as seasons,
                       -- Basic aggregates
                       SUM(g) as g, SUM(w) as w, SUM(l) as l, 
                       SUM(r) as r, SUM(ra) as ra,
                       -- We'll calculate playoff stats separately
                       0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
                FROM lahman_teams 
                WHERE teamid IN ({placeholders})
                """
                df = pd.read_sql_query(query, conn, params=franchise_ids)
            else:
                # Single team ID
                query = """
                SELECT teamid, 
                       COUNT(*) as seasons,
                       -- Basic aggregates
                       SUM(g) as g, SUM(w) as w, SUM(l) as l, 
                       SUM(r) as r, SUM(ra) as ra,
                       -- We'll calculate playoff stats separately
                       0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
                FROM lahman_teams 
                WHERE teamid = ?
                GROUP BY teamid
                """
                df = pd.read_sql_query(query, conn, params=(team_id,))

        else:
            # Default to season
            actual_year = year or 2024
            query = """
            SELECT yearid, teamid, 
                   g, w, l, r, ra,
                   0 as playoff_apps, 0 as ws_apps, 0 as ws_championships
            FROM lahman_teams 
            WHERE teamid = ? AND yearid = ?
            """
            df = pd.read_sql_query(query, conn, params=(team_id, actual_year))

        if not df.empty:
            # Add playoff statistics - pass actual_year for season mode
            df = add_playoff_stats(
                df, team_id, actual_year if mode == "season" else year, mode, conn
            )

        conn.close()
        if df.empty:
            if mode in ["franchise", "career", "overall"]:
                return (
                    jsonify({"error": f"Team '{team_id}' not found in database"}),
                    404,
                )
            else:
                return (
                    jsonify(
                        {"error": f"Team '{team_id}' not found for year {actual_year}"}
                    ),
                    404,
                )

        # Calculate derived stats
        df = calculate_simple_team_stats(df)

        # Pass the correct year value based on mode
        year_to_pass = actual_year if mode == "season" else None
        return format_combined_team_response(df, mode, team_id, year_to_pass)

    except Exception as e:
        import traceback

        traceback.print_exc()
        return jsonify({"error": f"Database error: {str(e)}"}), 500


def get_franchise_team_ids(team_id):
    """
    Map current team IDs to all historical team IDs for franchise totals
    This handles team moves and ID changes
    """
    franchise_mapping = {
        # Modern team ID -> All historical IDs for that franchise
        # Milwaukee Brewers - Current franchise (1970+)
        "MIL": ["MIL", "ML4"],  # NL Brewers (1998+) + AL Brewers (1970-1997)
        # Atlanta Braves - includes Boston Braves and Milwaukee Braves
        "ATL": ["ATL", "BSN", "ML1"],  # Atlanta + Boston + Milwaukee Braves (1953-1965)
        # Los Angeles Dodgers - includes all Brooklyn Dodgers
        "LAN": ["LAN", "BRO", "BR3"],
        # San Francisco Giants - includes New York Giants
        "SFN": ["SFN", "NY1"],
        # Baltimore Orioles - includes St. Louis Browns
        "BAL": ["BAL", "SLA", "MLA"],
        # Chicago White Sox
        "CHA": ["CHA"],
        # Cleveland Guardians/Indians
        "CLE": ["CLE"],
        # Cincinnati Reds
        "CIN": ["CIN", "CN2"],
        # Philadelphia Phillies
        "PHI": ["PHI"],
        # Oakland Athletics
        "OAK": [
            "OAK",
            "KC1",
            "PHA",
        ],
        # St. Louis Cardinals
        "SLN": ["SLN", "SL4"],
        # New York Yankees
        "NYA": ["NYA"],
        # New York Mets
        "NYN": ["NYN"],
        # Kansas City Royals
        "KCR": ["KCR"],
        # Minnesota Twins - includes original Washington Senators (1901-1960)
        "MIN": ["MIN", "WS1"],
        # Texas Rangers - includes expansion Washington Senators (1961-1971)
        "TEX": ["TEX", "WS2"],
        # Washington Nationals - includes Montreal Expos
        "WAS": ["WAS", "MON"],
        # Los Angeles Angels - various eras
        "LAA": ["LAA", "ANA", "CAL"],
        # Tampa Bay Rays
        "TBA": ["TBA", "TBD"],
        # Miami Marlins
        "MIA": ["MIA", "FLO", "FLA"],
        # Seattle Mariners
        "SEA": ["SEA"],
        # Pittsburgh Pirates
        "PIT": ["PIT", "PT1"],
        # Single-location franchises (no historical moves)
        "ARI": ["ARI"],
        "BOS": ["BOS"],
        "COL": ["COL"],
        "DET": ["DET"],
        "HOU": ["HOU"],
        "SDN": ["SDN"],
        "TOR": ["TOR"],
        "CHC": ["CHN"],
    }

    return franchise_mapping.get(team_id, [team_id])


def add_playoff_stats(df, team_id, year, mode, conn):
    """Add playoff appearance and World Series statistics using lahman_seriespost"""
    try:
        if mode == "season":
            # For single season, check if team made playoffs that year
            actual_year = year or 2024

            # Check if they appeared in any playoff series that year
            playoff_query = """
            SELECT COUNT(*) as series_count
            FROM lahman_seriespost 
            WHERE (teamidwinner = ? OR teamidloser = ?) 
            AND yearid = ?
            """

            playoff_df = pd.read_sql_query(
                playoff_query, conn, params=(team_id, team_id, actual_year)
            )
            playoff_apps = 1 if playoff_df.iloc[0]["series_count"] > 0 else 0

            # Check World Series appearances
            ws_query = """
            SELECT COUNT(*) as ws_series
            FROM lahman_seriespost 
            WHERE (teamidwinner = ? OR teamidloser = ?) 
            AND yearid = ? 
            AND round = 'WS'
            """

            ws_df = pd.read_sql_query(
                ws_query, conn, params=(team_id, team_id, actual_year)
            )
            ws_apps = 1 if ws_df.iloc[0]["ws_series"] > 0 else 0

            # Check World Series wins
            ws_win_query = """
            SELECT COUNT(*) as ws_wins
            FROM lahman_seriespost 
            WHERE teamidwinner = ?
            AND yearid = ? 
            AND round = 'WS'
            """

            ws_win_df = pd.read_sql_query(
                ws_win_query, conn, params=(team_id, actual_year)
            )
            ws_championships = ws_win_df.iloc[0]["ws_wins"]

        else:
            # For franchise/career mode, count all playoff appearances
            playoff_query = """
            SELECT COUNT(DISTINCT yearid) as playoff_years
            FROM lahman_seriespost 
            WHERE (teamidwinner = ? OR teamidloser = ?)
            """

            playoff_df = pd.read_sql_query(
                playoff_query, conn, params=(team_id, team_id)
            )
            playoff_apps = playoff_df.iloc[0]["playoff_years"]

            # Count World Series appearances
            ws_query = """
            SELECT COUNT(DISTINCT yearid) as ws_years
            FROM lahman_seriespost 
            WHERE (teamidwinner = ? OR teamidloser = ?) 
            AND round = 'WS'
            """

            ws_df = pd.read_sql_query(ws_query, conn, params=(team_id, team_id))
            ws_apps = ws_df.iloc[0]["ws_years"]

            # Count World Series championships
            ws_win_query = """
            SELECT COUNT(*) as total_ws_wins
            FROM lahman_seriespost 
            WHERE teamidwinner = ?
            AND round = 'WS'
            """

            ws_win_df = pd.read_sql_query(ws_win_query, conn, params=(team_id,))
            ws_championships = ws_win_df.iloc[0]["total_ws_wins"]

        df.loc[0, "playoff_apps"] = playoff_apps
        df.loc[0, "ws_apps"] = ws_apps
        df.loc[0, "ws_championships"] = ws_championships

        return df

    except Exception as e:
        import traceback

        traceback.print_exc()
        # Return original df with zeros for playoff stats
        df.loc[0, "playoff_apps"] = 0
        df.loc[0, "ws_apps"] = 0
        df.loc[0, "ws_championships"] = 0
        return df


def calculate_simple_team_stats(df):
    """Calculate basic team stats for StatHead format"""
    try:
        df.columns = df.columns.str.lower()

        # Essential columns for StatHead format
        essential_cols = ["g", "w", "l", "r", "ra"]

        for col in essential_cols:
            if col not in df.columns:
                df[col] = 0

        # Fill NaN and convert to numeric
        for col in essential_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Calculate derived stats
        df["gp"] = df["g"]  # Games played same as games
        df["rpg"] = df.apply(
            lambda row: row["r"] / row["g"] if row["g"] > 0 else 0, axis=1
        )  # Runs per game
        df["rapg"] = df.apply(
            lambda row: row["ra"] / row["g"] if row["g"] > 0 else 0, axis=1
        )  # Runs allowed per game

        return df

    except Exception as e:
        return df


def calculate_combined_team_stats(df):
    """Calculate both batting and pitching derived stats - without RBI"""
    try:
        df.columns = df.columns.str.lower()

        # Updated to include hbp and sf but exclude rbi
        batting_cols = ["ab", "h", "bb", "hbp", "sf", "2b", "3b", "hr", "r", "sb"]
        pitching_cols = ["ipouts", "er", "ha", "bba", "so_pitching", "w", "l"]

        for col in batting_cols + pitching_cols:
            if col not in df.columns:
                df[col] = 0

        # Fill NaN and convert to numeric
        for col in batting_cols + pitching_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Batting calculations
        df["ba"] = df.apply(
            lambda row: row["h"] / row["ab"] if row["ab"] > 0 else 0, axis=1
        )

        # Pitching calculations
        df["ip"] = df["ipouts"] / 3.0
        df["era_calc"] = df.apply(
            lambda row: (
                (row["er"] * 9) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0
            ),
            axis=1,
        )
        df["whip"] = df.apply(
            lambda row: (
                (row["ha"] + row["bba"]) / (row["ipouts"] / 3.0)
                if row["ipouts"] > 0
                else 0
            ),
            axis=1,
        )

        # Rename columns for consistency
        df = df.rename(
            columns={
                "ha": "h_allowed",
                "bba": "bb_allowed",
                "so_pitching": "so",  # Use pitching strikeouts as main SO stat
                "hra": "hr_allowed",
            }
        )

        return df

    except Exception as e:
        return df


def format_combined_team_response(df, mode, team_id, year):
    """Format combined team stats response"""
    try:
        # Pass the mode to get_team_name for proper formatting
        team_name = get_team_name(team_id, year, mode)

        stats = df.to_dict(orient="records")[0] if not df.empty else {}

        # Convert numpy types and apply formatting
        for key, value in stats.items():
            if hasattr(value, "item"):
                stats[key] = value.item()
            elif pd.isna(value):
                stats[key] = None

        stats = format_and_round_stats(stats)
        team_logo = get_team_logo_with_fallback(team_id, year)

        return jsonify(
            {
                "mode": mode,
                "team_id": team_id,
                "team_name": team_name,
                "year": year,
                "team_logo": team_logo,
                "stats": stats,
            }
        )

    except Exception as e:
        return jsonify({"error": f"Response formatting error: {str(e)}"}), 500


# Include all your existing helper functions here:
def parse_team_input(team):
    """Parse team input like '2024 Dodgers', 'Dodgers 2024', 'Yankees', etc."""
    try:
        parts = team.strip().split()

        if len(parts) == 1:
            team_code = get_team_code_from_search(parts[0])
            return team_code, None

        elif len(parts) == 2:
            if parts[0].isdigit():
                team_code = get_team_code_from_search(parts[1])
                return team_code, int(parts[0])
            elif parts[1].isdigit():
                team_code = get_team_code_from_search(parts[0])
                return team_code, int(parts[1])
            else:
                full_name = " ".join(parts)
                team_code = get_team_code_from_search(full_name)
                return team_code, None

        else:
            if parts[-1].isdigit() and len(parts[-1]) == 4:
                team_name = " ".join(parts[:-1])
                team_code = get_team_code_from_search(team_name)
                return team_code, int(parts[-1])
            elif parts[0].isdigit() and len(parts[0]) == 4:
                team_name = " ".join(parts[1:])
                team_code = get_team_code_from_search(team_name)
                return team_code, int(parts[0])
            else:
                full_name = " ".join(parts)
                team_code = get_team_code_from_search(full_name)
                return team_code, None

    except Exception as e:
        team_code = get_team_code_from_search(team)
        return team_code, None


def get_team_code_from_search(search_term):
    """Convert team search terms to database team codes"""
    search_term = search_term.lower().strip()

    # Your existing team mapping dictionaries here...
    # Direct team code mappings (if they search the exact code)
    team_codes = {
        # Angels - various historical codes
        "alt": "ALT",  # Early Angels
        "cal": "CAL",  # California Angels
        "ana": "ANA",  # Anaheim Angels
        "laa": "LAA",  # Los Angeles Angels (current)
        # Diamondbacks
        "ari": "ARI",
        # Braves - historical codes
        "bsn": "BSN",  # Boston Braves (historical)
        "ml1": "ML1",  # Milwaukee Braves (historical)
        "mla": "MLA",  # Another Milwaukee code
        "atl": "ATL",  # Atlanta Braves (current)
        # Orioles - historical St. Louis Browns
        "sla": "SLA",  # St. Louis Browns (historical)
        "bal": "BAL",  # Baltimore Orioles (current)
        # Red Sox
        "bs1": "BS1",  # Historical Boston code
        "bos": "BOS",  # Boston Red Sox (current)
        # Cubs
        "chn": "CHN",  # Chicago Cubs (historical database code)
        "chc": "CHC",  # Chicago Cubs (modern)
        # White Sox
        "cha": "CHA",  # Chicago White Sox (historical database code)
        "chw": "CWS",  # Map CHW to CWS
        "cws": "CWS",  # Chicago White Sox (current)
        # Reds
        "cn2": "CN2",  # Historical Cincinnati code
        "cn3": "CN3",  # Another historical Cincinnati code
        "cin": "CIN",  # Cincinnati Reds (current)
        # Indians/Guardians
        "cle": "CLE",  # Cleveland (current - covers both Indians and Guardians)
        # Rockies
        "col": "COL",
        # Tigers
        "det": "DET",
        # Astros
        "hou": "HOU",
        # Royals
        "kca": "KCA",  # Map old code to current
        "kcr": "KCA",  # Kansas City Royals
        # Dodgers
        "br1": "BR1",  # Brooklyn Dodgers (very old)
        "br2": "BR2",  # Brooklyn Dodgers (historical)
        "br4": "BR4",  # Brooklyn Dodgers (another historical)
        "bro": "BRO",  # Brooklyn (if it exists)
        "lad": "LAN",  # Map LAD to LAN (database uses LAN)
        "lan": "LAN",  # Los Angeles Dodgers (database code)
        # Marlins
        "fla": "FLA",  # Florida Marlins (historical)
        "mia": "MIA",  # Miami Marlins (current)
        # Brewers
        "mil": "MIL",
        # Twins
        "min": "MIN",
        # Mets
        "nym": "NYN",
        # Yankees
        "nyy": "NYA",
        # Athletics
        "pha": "PHA",  # Philadelphia Athletics (historical)
        "oak": "OAK",  # Oakland Athletics (current)
        # Phillies
        "phn": "PHN",  # Historical Philadelphia code
        "ph3": "PH3",  # Another historical Philadelphia code
        "phi": "PHI",  # Philadelphia Phillies (current)
        # Pirates
        "pit": "PIT",
        # Padres
        "sdp": "SDN",  # San Diego Padres (database code)
        "sd": "SDN",  # Map SD to SDP
        # Mariners
        "sea": "SEA",
        # Giants
        "ny1": "NY1",  # New York Giants (historical, if it exists)
        "sfg": "SFN",  # San Francisco Giants (database code)
        "sf": "SFN",  # Map SF to SFG
        # Cardinals
        "stl": "SLN",
        # Rays
        "tbd": "TBA",  # Map old code to current
        "TBA": "TBA",  # Tampa Bay Rays
        "tb": "TBA",  # Map TB to TBR
        # Rangers
        "was": "WAS",  # Washington Senators (historical, became Rangers)
        "tex": "TEX",  # Texas Rangers (current)
        # Blue Jays
        "tor": "TOR",
        # Nationals
        "mon": "MON",  # Montreal Expos (historical)
        "wsn": "WAS",  # Washington Nationals (current)
        "was": "WAS",  # Map WAS to WSN for Nationals
    }

    if search_term in team_codes:
        return team_codes[search_term]

    # Name mappings
    name_mappings = {
        # Angels
        "angels": "LAA",
        "los angeles angels": "LAA",
        "anaheim angels": "ANA",  # Use historical code for historical name
        "california angels": "CAL",  # Use historical code for historical name
        # Diamondbacks
        "diamondbacks": "ARI",
        "arizona diamondbacks": "ARI",
        "d-backs": "ARI",
        "dbacks": "ARI",
        # Braves
        "braves": "ATL",
        "atlanta braves": "ATL",
        "atlanta": "ATL",
        "boston braves": "BSN",  # Historical
        "milwaukee braves": "ML1",  # Historical
        # Orioles
        "orioles": "BAL",
        "baltimore orioles": "BAL",
        "baltimore": "BAL",
        "o's": "BAL",
        "st. louis browns": "SLA",
        "st louis browns": "SLA",
        "browns": "SLA",
        # Red Sox
        "red sox": "BOS",
        "boston red sox": "BOS",
        "boston": "BOS",
        "redsox": "BOS",
        # Cubs
        "cubs": "CHN",
        "chicago cubs": "CHN",
        "cubbies": "CHN",
        # White Sox
        "white sox": "CHA",
        "chicago white sox": "CHA",
        "whitesox": "CHA",
        # Reds
        "reds": "CIN",
        "cincinnati reds": "CIN",
        "cincinnati": "CIN",
        # Guardians/Indians
        "guardians": "CLE",
        "cleveland guardians": "CLE",
        "cleveland": "CLE",
        "indians": "CLE",
        "cleveland indians": "CLE",
        # Rockies
        "rockies": "COL",
        "colorado rockies": "COL",
        "colorado": "COL",
        # Tigers
        "tigers": "DET",
        "detroit tigers": "DET",
        "detroit": "DET",
        # Astros
        "astros": "HOU",
        "houston astros": "HOU",
        "houston": "HOU",
        # Royals
        "royals": "KCA",
        "kansas city royals": "KCA",
        "kansas city": "KCA",
        # Dodgers
        "dodgers": "LAN",
        "los angeles dodgers": "LAN",
        "brooklyn dodgers": "BR2",
        # Marlins
        "marlins": "MIA",
        "miami marlins": "MIA",
        "florida marlins": "FLA",
        "miami": "MIA",
        # Brewers
        "brewers": "MIL",
        "milwaukee brewers": "MIL",
        "milwaukee": "MIL",
        # Twins
        "twins": "MIN",
        "minnesota twins": "MIN",
        "minnesota": "MIN",
        # Mets
        "mets": "NYN",
        "new york mets": "NYN",
        "ny mets": "NYN",
        # Yankees
        "yankees": "NYA",
        "new york yankees": "NYA",
        "ny yankees": "NYA",
        "yanks": "NYA",
        # Athletics
        "athletics": "OAK",
        "oakland athletics": "OAK",
        "oakland": "OAK",
        "a's": "OAK",
        "as": "OAK",
        "philadelphia athletics": "PHA",
        # Phillies
        "phillies": "PHI",
        "philadelphia phillies": "PHI",
        "philadelphia": "PHI",
        "phils": "PHI",
        # Pirates
        "pirates": "PIT",
        "pittsburgh pirates": "PIT",
        "pittsburgh": "PIT",
        "bucs": "PIT",
        # Padres
        "padres": "SDN",
        "san diego padres": "SDN",
        "san diego": "SDN",
        # Mariners
        "mariners": "SEA",
        "seattle mariners": "SEA",
        "seattle": "SEA",
        "m's": "SEA",
        # Giants
        "giants": "SFN",
        "san francisco giants": "SFN",
        "sf giants": "SFN",
        "san francisco": "SFN",
        # Cardinals
        "cardinals": "SLN",
        "st. louis cardinals": "SLN",
        "st louis cardinals": "SLN",
        "cards": "SLN",
        "st. louis": "SLN",
        "st louis": "SLN",
        # Rays
        "rays": "TBA",
        "tampa bay rays": "TBA",
        "tampa bay": "TBA",
        "devil rays": "TBD",
        "tampa bay devil rays": "TBD",
        # Rangers
        "rangers": "TEX",
        "texas rangers": "TEX",
        "texas": "TEX",
        "washington senators": "WAS",
        "senators": "WAS",
        # Blue Jays
        "blue jays": "TOR",
        "toronto blue jays": "TOR",
        "toronto": "TOR",
        "jays": "TOR",
        "bluejays": "TOR",
        # Nationals
        "nationals": "WAS",
        "washington nationals": "WAS",
        "washington": "WAS",
        "nats": "WAS",
        "expos": "MON",
        "montreal expos": "MON",
    }

    if search_term in name_mappings:
        return name_mappings[search_term]

    for name, code in name_mappings.items():
        if search_term in name or name in search_term:
            return code

    return search_term.upper()


def get_team_name(team_id, year=None, mode=None):
    """Get full team name for display"""
    team_names = {
        # Angels - various eras
        "ALT": "Los Angeles Angels",  # Early Angels
        "CAL": "California Angels",  # California era
        "ANA": "Anaheim Angels",  # Anaheim era
        "LAA": "Los Angeles Angels",  # Current era
        # Diamondbacks
        "ARI": "Arizona Diamondbacks",
        # Braves - moved cities
        "BSN": "Boston Braves",  # Boston era
        "ML1": "Milwaukee Braves",  # Milwaukee era
        "MLA": "Milwaukee Braves",  # Alternative Milwaukee code
        "ATL": "Atlanta Braves",  # Current Atlanta era
        # Orioles / St. Louis Browns
        "SLA": "St. Louis Browns",  # Before moving to Baltimore
        "BAL": "Baltimore Orioles",  # Current
        # Red Sox
        "BS1": "Boston Red Sox",  # Historical code
        "BOS": "Boston Red Sox",  # Current
        # Cubs
        "CHN": "Chicago Cubs",  # Database code
        "CHC": "Chicago Cubs",  # Alternative
        # White Sox
        "CHA": "Chicago White Sox",  # Database code
        "CHW": "Chicago White Sox",  # Alternative
        "CWS": "Chicago White Sox",  # Alternative
        # Reds
        "CN2": "Cincinnati Reds",  # Historical code
        "CN3": "Cincinnati Reds",  # Another historical code
        "CIN": "Cincinnati Reds",  # Current
        # Indians/Guardians
        "CLE": "Cleveland Guardians",  # Current (covers both Indians/Guardians)
        # Rockies
        "COL": "Colorado Rockies",
        # Tigers
        "DET": "Detroit Tigers",
        # Astros
        "HOU": "Houston Astros",
        # Royals
        "KCA": "Kansas City Royals",  # Historical code
        # Dodgers - Brooklyn to LA
        "BR1": "Brooklyn Dodgers",  # Early Brooklyn era
        "BR2": "Brooklyn Dodgers",  # Brooklyn era
        "BR4": "Brooklyn Dodgers",  # Another Brooklyn code
        "BRO": "Brooklyn Dodgers",  # If this code exists
        "LAD": "Los Angeles Dodgers",  # Alternative current
        "LAN": "Los Angeles Dodgers",  # Database code
        # Marlins
        "FLA": "Florida Marlins",  # Florida era
        "MIA": "Miami Marlins",  # Current Miami era
        # Brewers
        "MIL": "Milwaukee Brewers",
        # Twins
        "MIN": "Minnesota Twins",
        # Mets
        "NYN": "New York Mets",
        # Yankees
        "NYA": "New York Yankees",
        # Athletics - Philadelphia to Oakland
        "PHA": "Philadelphia Athletics",  # Philadelphia era
        "OAK": "Oakland Athletics",  # Current Oakland era
        # Phillies
        "PHN": "Philadelphia Phillies",  # Historical code
        "PH3": "Philadelphia Phillies",  # Another historical code
        "PHI": "Philadelphia Phillies",  # Current
        # Pirates
        "PIT": "Pittsburgh Pirates",
        # Padres
        "SDN": "San Diego Padres",  # Database code
        # Mariners
        "SEA": "Seattle Mariners",
        # Giants - New York to San Francisco
        "NY1": "New York Giants",  # If this code exists
        "SFN": "San Francisco Giants",  # Current
        "SF": "San Francisco Giants",  # Alternative
        # Cardinals
        "SLN": "St. Louis Cardinals",
        # Rays
        "TBD": "Tampa Bay Devil Rays",  # Devil Rays era
        "TBA": "Tampa Bay Rays",  # Current Rays era
        "TB": "Tampa Bay Rays",  # Alternative
        # Rangers / Washington Senators
        "WAS": "Washington Senators",  # Before moving to Texas
        "TEX": "Texas Rangers",  # Current
        # Blue Jays
        "TOR": "Toronto Blue Jays",
        # Nationals / Montreal Expos
        "MON": "Montreal Expos",  # Montreal era
        "WSN": "Washington Nationals",  # Current
        "WAS": "Washington Nationals",  # Alternative (though might conflict with Senators)
    }

    base_name = team_names.get(team_id.upper(), team_id)

    # Check mode first, then year
    if mode == "season" and year is not None:
        return f"{year} {base_name}"
    elif mode in ["franchise", "career", "overall"] or year is None:
        return f"{base_name} (All-Time)"
    else:
        # Fallback for when year is provided but mode is unclear
        return f"{year} {base_name}"


def get_team_logo_url(team_id, year=None):
    """Get team logo URL using working MLB logo sources"""

    # Map your database team codes to MLB's team IDs and abbreviations
    mlb_team_mapping = {
        # Angels
        "ALT": {"abbrev": "LAA", "id": "108"},
        "CAL": {"abbrev": "LAA", "id": "108"},
        "ANA": {"abbrev": "LAA", "id": "108"},
        "LAA": {"abbrev": "LAA", "id": "108"},
        # Diamondbacks
        "ARI": {"abbrev": "ARI", "id": "109"},
        # Braves
        "BSN": {"abbrev": "ATL", "id": "144"},
        "ML1": {"abbrev": "ATL", "id": "144"},
        "MLA": {"abbrev": "ATL", "id": "144"},
        "ATL": {"abbrev": "ATL", "id": "144"},
        # Orioles
        "SLA": {"abbrev": "BAL", "id": "110"},
        "BAL": {"abbrev": "BAL", "id": "110"},
        # Red Sox
        "BS1": {"abbrev": "BOS", "id": "111"},
        "BOS": {"abbrev": "BOS", "id": "111"},
        # Cubs
        "CHN": {"abbrev": "CHC", "id": "112"},
        "CHC": {"abbrev": "CHC", "id": "112"},
        # White Sox
        "CHA": {"abbrev": "CWA", "id": "145"},
        "CHW": {"abbrev": "CHA", "id": "145"},
        "CWS": {"abbrev": "CWA", "id": "145"},
        # Reds
        "CN2": {"abbrev": "CIN", "id": "113"},
        "CN3": {"abbrev": "CIN", "id": "113"},
        "CIN": {"abbrev": "CIN", "id": "113"},
        # Indians/Guardians
        "CLE": {"abbrev": "CLE", "id": "114"},
        # Rockies
        "COL": {"abbrev": "COL", "id": "115"},
        # Tigers
        "DET": {"abbrev": "DET", "id": "116"},
        # Astros
        "HOU": {"abbrev": "HOU", "id": "117"},
        # Royals
        "KCA": {"abbrev": "KCA", "id": "118"},
        #'KCR': {'abbrev': 'KCA', 'id': '118'},
        # Dodgers
        "BR1": {"abbrev": "LAD", "id": "119"},
        "BR2": {"abbrev": "LAD", "id": "119"},
        "BR4": {"abbrev": "LAD", "id": "119"},
        "BRO": {"abbrev": "LAD", "id": "119"},
        "LAD": {"abbrev": "LAD", "id": "119"},
        "LAN": {"abbrev": "LAD", "id": "119"},
        # Marlins
        "FLA": {"abbrev": "MIA", "id": "146"},
        "MIA": {"abbrev": "MIA", "id": "146"},
        # Brewers
        "MIL": {"abbrev": "MIL", "id": "158"},
        # Twins
        "MIN": {"abbrev": "MIN", "id": "142"},
        # Mets
        "NYN": {"abbrev": "NYN", "id": "121"},
        # Yankees
        "NYA": {"abbrev": "NYA", "id": "147"},
        # Athletics
        "PHA": {"abbrev": "OAK", "id": "133"},
        "OAK": {"abbrev": "OAK", "id": "133"},
        # Phillies
        "PHN": {"abbrev": "PHI", "id": "143"},
        "PH3": {"abbrev": "PHI", "id": "143"},
        "PHI": {"abbrev": "PHI", "id": "143"},
        # Pirates
        "PIT": {"abbrev": "PIT", "id": "134"},
        # Padres
        "SDP": {"abbrev": "SDN", "id": "135"},
        "SD": {"abbrev": "SDN", "id": "135"},
        # Mariners
        "SEA": {"abbrev": "SEA", "id": "136"},
        # Giants
        "NY1": {"abbrev": "SFN", "id": "137"},
        "SFN": {"abbrev": "SFN", "id": "137"},
        "SF": {"abbrev": "SFN", "id": "137"},
        # Cardinals
        "SLN": {"abbrev": "SLN", "id": "138"},
        # Rays
        "TBD": {"abbrev": "TB", "id": "139"},
        "TBA": {"abbrev": "TB", "id": "139"},
        "TB": {"abbrev": "TB", "id": "139"},
        # Rangers
        "WAS": {"abbrev": "TEX", "id": "140"},
        "TEX": {"abbrev": "TEX", "id": "140"},
        # Blue Jays
        "TOR": {"abbrev": "TOR", "id": "141"},
        # Nationals
        "MON": {"abbrev": "WSH", "id": "120"},
        "WSN": {"abbrev": "WSH", "id": "120"},
        "WAS": {"abbrev": "WSH", "id": "120"},
    }

    # Get team info
    team_info = mlb_team_mapping.get(
        team_id.upper(), {"abbrev": team_id.upper(), "id": "0"}
    )
    abbrev = team_info["abbrev"]
    team_number = team_info["id"]

    # Try multiple URL patterns that are known to work
    logo_urls = [
        # MLB official team logos - using team ID
        f"https://www.mlbstatic.com/team-logos/url/{team_number}.svg",
        # Alternative MLB static URLs
        f"https://img.mlbstatic.com/mlb-photos/image/upload/v1/team/{abbrev.lower()}/logo/current",
        # ESPN logos (reliable fallback)
        f"https://a.espncdn.com/i/teamlogos/mlb/500/{abbrev.lower()}.png",
        # Sports logos database
        f"https://content.sportslogos.net/logos/54/{team_number}/{abbrev}-logo-primary-dark.png",
        # Loodibee logos (free transparent PNGs)
        f"https://loodibee.com/wp-content/uploads/mlb-{abbrev.lower()}-logo-transparent.png",
        # TeamColorCodes fallback
        f"https://teamcolorcodes.com/wp-content/uploads/{abbrev.lower()}-logo.png",
    ]

    return logo_urls[0]  # Return primary URL


def get_team_logo_with_fallback(team_id, year=None):
    """Get team logo with fallback options"""
    mlb_team_mapping = {
        # Fixed mappings - database code to ESPN abbreviation
        "CHN": {"abbrev": "chc"},  # Cubs
        "CHA": {"abbrev": "cws"},  # White Sox
        "LAN": {"abbrev": "lad"},  # Dodgers
        "SLN": {"abbrev": "stl"},  # Cardinals - FIXED
        "SDN": {"abbrev": "sd"},  # Padres - FIXED
        "SFN": {"abbrev": "sf"},  # Giants - FIXED
        "NYN": {"abbrev": "nym"},  # Mets - FIXED
        "NYA": {"abbrev": "nyy"},  # Yankees - FIXED
        "KCA": {"abbrev": "kc"},  # Royals - FIXED
        "WAS": {"abbrev": "wsh"},  # Nationals - FIXED
        "TBA": {"abbrev": "tb"},  # Rays
        "WSN": {"abbrev": "wsh"},  # Alternative Nationals code
        # Other teams for completeness
        "LAA": {"abbrev": "laa"},  # Angels
        "ARI": {"abbrev": "ari"},  # Diamondbacks
        "ATL": {"abbrev": "atl"},  # Braves
        "BAL": {"abbrev": "bal"},  # Orioles
        "BOS": {"abbrev": "bos"},  # Red Sox
        "CIN": {"abbrev": "cin"},  # Reds
        "CLE": {"abbrev": "cle"},  # Guardians
        "COL": {"abbrev": "col"},  # Rockies
        "DET": {"abbrev": "det"},  # Tigers
        "HOU": {"abbrev": "hou"},  # Astros
        "MIL": {"abbrev": "mil"},  # Brewers
        "MIN": {"abbrev": "min"},  # Twins
        "OAK": {"abbrev": "oak"},  # Athletics
        "PHI": {"abbrev": "phi"},  # Phillies
        "PIT": {"abbrev": "pit"},  # Pirates
        "SEA": {"abbrev": "sea"},  # Mariners
        "TEX": {"abbrev": "tex"},  # Rangers
        "TOR": {"abbrev": "tor"},  # Blue Jays
        "MIA": {"abbrev": "mia"},  # Marlins
    }

    team_info = mlb_team_mapping.get(team_id.upper(), {"abbrev": team_id.lower()})
    abbrev = team_info["abbrev"]

    primary_url = f"https://a.espncdn.com/i/teamlogos/mlb/500/{abbrev}.png"

    fallback_urls = [
        f"https://loodibee.com/wp-content/uploads/mlb-{abbrev}-logo-transparent.png",
        f"https://content.sportslogos.net/logos/54/team/{abbrev}-logo-primary-dark.png",
    ]

    return {"primary": primary_url, "fallbacks": fallback_urls}


def format_and_round_stats(stats_dict):
    """Format stats with proper decimal places - updated for StatHead format"""

    # Stats that should show one decimal place
    per_game_stats = ["rpg", "rapg"]

    formatted_stats = {}

    for key, value in stats_dict.items():
        if value is None or pd.isna(value):
            formatted_stats[key] = None
            continue

        try:
            num_value = float(value)
        except (ValueError, TypeError):
            formatted_stats[key] = value
            continue

        if key in per_game_stats:
            # Per-game stats get 1 decimal place
            formatted_stats[key] = f"{num_value:.1f}"
        else:
            # Everything else is whole numbers
            if isinstance(num_value, float) and num_value.is_integer():
                formatted_stats[key] = int(num_value)
            elif isinstance(num_value, float):
                formatted_stats[key] = int(round(num_value))
            else:
                formatted_stats[key] = value

    return formatted_stats


def get_head_to_head_record(team_a, team_b, year_filter=None):
    """
    Get head-to-head record between two teams using Lahman database structure
    and Retrosheet teamstats for regular season games
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Get regular season head-to-head from Retrosheet teamstats
        regular_season_record = get_regular_season_h2h(
            conn, team_a, team_b, year_filter
        )

        # Get playoff data (your existing code)
        playoff_query = """
        SELECT yearid, round, teamidwinner, teamidloser, wins, losses
        FROM lahman_seriespost 
        WHERE (
            (teamidwinner = ? AND teamidloser = ?) OR 
            (teamidwinner = ? AND teamidloser = ?)
        )
        """

        params = [team_a, team_b, team_b, team_a]

        if year_filter:
            playoff_query += " AND yearid = ?"
            params.append(year_filter)

        playoff_df = pd.read_sql_query(playoff_query, conn, params=params)

        # Process playoff data (your existing logic)
        team_a_series_wins = len(playoff_df[playoff_df["teamidwinner"] == team_a])
        team_b_series_wins = len(playoff_df[playoff_df["teamidwinner"] == team_b])

        team_a_game_wins = 0
        team_b_game_wins = 0

        if "wins" in playoff_df.columns and "losses" in playoff_df.columns:
            for _, row in playoff_df.iterrows():
                wins_val = int(row["wins"]) if pd.notna(row["wins"]) else 0
                losses_val = int(row["losses"]) if pd.notna(row["losses"]) else 0

                if row["teamidwinner"] == team_a:
                    team_a_game_wins += wins_val
                    team_b_game_wins += losses_val
                else:
                    team_b_game_wins += wins_val
                    team_a_game_wins += losses_val

        series_details = []
        for _, row in playoff_df.iterrows():
            series_details.append(
                {
                    "year": int(row["yearid"]),
                    "round": row["round"],
                    "winner": row["teamidwinner"],
                    "loser": row["teamidloser"],
                    "series_wins": int(row["wins"]) if pd.notna(row["wins"]) else None,
                    "series_losses": (
                        int(row["losses"]) if pd.notna(row["losses"]) else None
                    ),
                }
            )

        playoff_record = {
            "series_wins": {"team_a": team_a_series_wins, "team_b": team_b_series_wins},
            "game_wins": {"team_a": team_a_game_wins, "team_b": team_b_game_wins},
            "series_details": series_details,
        }

        conn.close()

        return {
            "regular_season": regular_season_record,
            "playoffs": playoff_record,
            "note": "Regular season from Retrosheet teamstats, playoff records from Lahman database.",
        }

    except Exception as e:
        return {
            "regular_season": {"team_a_wins": 0, "team_b_wins": 0, "ties": 0},
            "playoffs": {"series_wins": {"team_a": 0, "team_b": 0}},
            "error": str(e),
        }


def get_regular_season_h2h(conn, team_a, team_b, year_filter=None):
    """
    Get regular season head-to-head record from retrosheet_teamstats
    Handles franchise moves and team ID changes
    """
    try:

        team_a_ids = get_franchise_team_ids(team_a)
        team_b_ids = get_franchise_team_ids(team_b)

        # Column mapping (simplified)
        team_col = "team"
        opponent_col = "opp"
        year_col = "date"
        win_col = "win"

        # Build dynamic query with franchise IDs
        team_a_placeholders = ",".join(["?" for _ in team_a_ids])
        team_b_placeholders = ",".join(["?" for _ in team_b_ids])

        base_query = f"""
        SELECT {team_col}, {opponent_col}, {year_col}, {win_col}
        FROM retrosheet_teamstats 
        WHERE (
            ({team_col} IN ({team_a_placeholders}) AND {opponent_col} IN ({team_b_placeholders})) OR 
            ({team_col} IN ({team_b_placeholders}) AND {opponent_col} IN ({team_a_placeholders}))
        )
        """

        # Build parameter list
        params = team_a_ids + team_b_ids + team_b_ids + team_a_ids

        if year_filter:
            base_query += f" AND substr({year_col}, 1, 4) = ?"
            params.append(str(year_filter))

        base_query += f" ORDER BY {year_col}, {team_col}"

        games_df = pd.read_sql_query(base_query, conn, params=params)

        if games_df.empty:
            return {"team_a_wins": 0, "team_b_wins": 0, "ties": 0, "total_games": 0}

        # Count wins for each franchise
        team_a_wins = 0
        team_b_wins = 0

        for _, row in games_df.iterrows():
            game_winner = row[team_col]
            game_win_flag = row[win_col]

            if game_win_flag == 1:  # This team won
                if game_winner in team_a_ids:
                    team_a_wins += 1
                elif game_winner in team_b_ids:
                    team_b_wins += 1

        total_games = len(games_df) // 2

        return {
            "team_a_wins": team_a_wins,
            "team_b_wins": team_b_wins,
            "ties": 0,
            "total_games": total_games,
        }

    except Exception as e:
        return {
            "team_a_wins": 0,
            "team_b_wins": 0,
            "ties": 0,
            "total_games": 0,
            "error": str(e),
        }


@app.route("/team/h2h")
def get_team_head_to_head():
    """Get head-to-head record between two teams"""
    try:
        team_a = request.args.get("team_a", "").strip()
        team_b = request.args.get("team_b", "").strip()
        year = request.args.get("year")  # Optional year filter

        if not team_a or not team_b:
            return jsonify({"error": "Both team_a and team_b required"}), 400

        # Parse team inputs
        team_a_id, _ = parse_team_input(team_a)
        team_b_id, _ = parse_team_input(team_b)

        year_filter = None
        if year and year.isdigit():
            year_filter = int(year)

        h2h_record = get_head_to_head_record(team_a_id, team_b_id, year_filter)

        # Get team names for better display
        team_a_name = get_team_name(team_a_id)
        team_b_name = get_team_name(team_b_id)

        return jsonify(
            {
                "team_a": {"id": team_a_id, "name": team_a_name},
                "team_b": {"id": team_b_id, "name": team_b_name},
                "year_filter": year_filter,
                "head_to_head": h2h_record,
                "status": "success",
            }
        )

    except Exception as e:
        return jsonify({"error": f"H2H lookup error: {str(e)}"}), 500


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)