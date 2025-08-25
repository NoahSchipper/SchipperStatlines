from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from pybaseball import playerid_lookup, cache
# from pybaseball import batting_stats, pitching_stats
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from supabase import create_client, Client

app = Flask(__name__, static_folder="static")

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DATABASE_URL = os.environ.get('DATABASE_URL')  # From Supabase settings

CORS(app, resources={
    r"/*": {
        "origins": [ "https://schipperstatlines.onrender.com", "https://website-a7a.pages.dev", "http://127.0.0.1:5501", "https://noahschipper.net"],
    }
})

cache.enable()

def get_db_connection():
    """Get PostgreSQL connection using psycopg2"""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

def get_supabase_client():
    """Get Supabase client for easier operations"""
    return supabase


''' (Team Endpoint -- still included for completeness, not used in this version)
KNOWN_TWO_WAY_PLAYERS = {
    # Modern era two-way players
    "ohtansh01": "Shohei Ohtani",
    # Historical two-way players (primarily known for both)
    "ruthba01": "Babe Ruth",
    # Players who had significant time as both (adjust as needed)
    "rickmri01": "Rick Ankiel",  # Started as pitcher, became position player
    "martipe02": "Pedro Martinez",  # Some hitting early in career
    # Add more as you identify them
    # Format: 'playerid': 'Display Name'
}


def is_predefined_two_way_player(playerid):
    """Check if player is in our predefined list of two-way players"""
    return playerid in KNOWN_TWO_WAY_PLAYERS


def detect_two_way_player_simple(playerid, conn):
    """Simplified two-way detection using only predefined list"""
    if is_predefined_two_way_player(playerid):
        return "two-way"

    # For all other players, use the original detection logic
    return detect_player_type(playerid, conn)


def get_photo_url_for_player(playerid, conn):
    """Get photo URL using the correct player info from database"""
    try:
        # Get the actual names and debut info from the database for this specific playerid
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT namefirst, namelast, debut, finalgame, birthyear 
            FROM lahman_people WHERE playerid = %s
        """,
            (playerid,),
        )
        name_result = cursor.fetchone()

        if not name_result:
            return None

        db_first, db_last, debut, final_game, birth_year = name_result

        # Special handling for known tricky cases with direct MLB ID mappings
        direct_mlb_mappings = {
            # Format: playerid -> (search_name, mlb_id)
            "griffke01": ("Ken Griffey", None),  # Sr. - let lookup handle it
            "griffke02": ("Ken Griffey Jr.", None),  # Jr. - let lookup handle it
            "tatisfe01": ("Fernando Tatis", 123107),  # Sr. with direct MLB ID
            "tatisfe02": ("Fernando Tatis Jr.", 665487),  # Jr. with direct MLB ID
            "ripkeca99": ("Cal Ripken Sr.", None),  # Sr. - was mostly coach/manager
            "ripkeca01": ("Cal Ripken Jr.", 121222),  # Jr. with direct MLB ID
            "raineti01": ("Tim Raines", 120891),  # Sr. with direct MLB ID
            "raineti02": ("Tim Raines Jr.", 406428),  # Jr. with direct MLB ID
            "alomasa01": ("Sandy Alomar Sr.", None),  # Sr.
            "alomasa02": ("Sandy Alomar Jr.", None),  # Jr.
            "rosepe01": ("Pete Rose", None),  # Sr. - let lookup handle it
            "rosepe02": ("Pete Rose Jr.", 121453),  # Jr. with direct MLB ID
            "baezja01": ("Javier Baez", 595879),  # Direct MLB ID for Baez
        }

        # Check if we have a direct mapping for this player
        if playerid in direct_mlb_mappings:
            search_name, direct_mlb_id = direct_mlb_mappings[playerid]

            # If we have a direct MLB ID, use it
            if direct_mlb_id:
                photo_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/v1/people/{direct_mlb_id}/headshot/67/current.jpg"
                return photo_url
        else:
            # Not a known special case, use database names
            search_name = f"{db_first} {db_last}"

        # For cases without direct MLB ID, do the lookup
        mlb_id = None
        try:
            # Split the search name for the lookup function
            name_parts = (
                search_name.replace(" Jr.", "")
                .replace(" Sr.", "")
                .replace(" III", "")
                .replace(" II", "")
                .split()
            )
            lookup_first = name_parts[0]
            lookup_last = " ".join(name_parts[1:])
            lookup = playerid_lookup(lookup_last, lookup_first)

            if not lookup.empty:

                # For father/son cases, try to match by career years
                best_match = None
                if len(lookup) > 1:
                    debut_year = int(debut[:4]) if debut else None
                    final_year = int(final_game[:4]) if final_game else None

                    for _, row in lookup.iterrows():
                        if pd.isna(row["key_mlbam"]):
                            continue

                        mlb_first = row.get("mlb_played_first")
                        mlb_last = row.get("mlb_played_last")

                        # Try to match by career overlap
                        if debut_year and mlb_first and mlb_last:
                            mlb_first_year = int(mlb_first) if mlb_first else None
                            mlb_last_year = int(mlb_last) if mlb_last else None

                            if (
                                mlb_first_year
                                and abs(mlb_first_year - debut_year) <= 1
                                and mlb_last_year
                                and final_year
                                and abs(mlb_last_year - final_year) <= 1
                            ):
                                best_match = row
                                break

                # Use best match or first available
                target_row = best_match if best_match is not None else lookup.iloc[0]

                if not pd.isna(target_row["key_mlbam"]):
                    mlb_id = int(target_row["key_mlbam"])
                else:
                    print("MLB ID is NaN")
            else:
                print("No lookup results found")

        except Exception as e:
            print(f"MLB ID lookup failed for {search_name}: {e}")

        if mlb_id:
            photo_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/v1/people/{mlb_id}/headshot/67/current.jpg"
            return photo_url

        return None

    except Exception as e:
        return None


def get_world_series_championships(playerid, conn):
    """Get World Series championships for a player"""
    try:
        cursor = conn.cursor()

        # Method 1: Check if player's team won World Series in years they played
        ws_query = """
        SELECT DISTINCT b.yearid, b.teamid, s.name as team_name
        FROM lahman_batting b
        JOIN lahman_seriespost sp ON b.yearid = sp.yearid AND b.teamid = sp.teamidwinner
        LEFT JOIN lahman_teams s ON b.teamid = s.teamid AND b.yearid = s.yearid
        WHERE b.playerid = %s AND sp.round = 'WS'
        
        UNION
        
        SELECT DISTINCT p.yearid, p.teamid, s.name as team_name
        FROM lahman_pitching p
        JOIN lahman_seriespost sp ON p.yearid = sp.yearid AND p.teamid = sp.teamidwinner
        LEFT JOIN lahman_teams s ON p.teamid = s.teamid AND p.yearid = s.yearid
        WHERE p.playerid = %s AND sp.round = 'WS'
        
        ORDER BY 1 DESC
        """

        cursor.execute(ws_query, (playerid, playerid))
        ws_results = cursor.fetchall()

        championships = []
        for row in ws_results:
            year, team_id, team_name = row
            championships.append(
                {"year": year, "team": team_id, "team_name": team_name or team_id}
            )

        return championships

    except Exception as e:
        # Fallback: check awards table for WS entries
        try:
            cursor.execute(
                """
                SELECT yearid, notes
                FROM lahman_awardsplayers 
                WHERE playerid = %s AND awardid = 'WS'
                ORDER BY yearid DESC
            """,
                (playerid,),
            )

            fallback_results = cursor.fetchall()
            championships = []
            for year, notes in fallback_results:
                championships.append(
                    {
                        "year": year,
                        "team": "Unknown",
                        "team_name": notes or "World Series Champion",
                    }
                )

            return championships

        except Exception as e2:
            return []


def get_career_war(playerid):
    """Get career WAR from JEFFBAGWELL database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT SUM(WAR162) as career_war 
            FROM jeffbagwell_war 
            WHERE key_bbref = %s
        """,
            (playerid,),
        )

        result = cursor.fetchone()
        conn.close()

        if result and result[0] is not None:
            return float(result[0])
        return 0.0

    except Exception as e:
        return 0.0


def get_season_war_history(playerid):
    """Get season-by-season WAR from JEFFBAGWELL database"""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(
            """
            SELECT year_ID, WAR162 as war
            FROM jeffbagwell_war 
            WHERE key_bbref = %s
            ORDER BY year_ID DESC
        """,
            conn,
            params=(playerid,),
        )
        conn.close()
        return df

    except Exception as e:
        return pd.DataFrame()


def detect_player_type(playerid, conn):
    """Detect if player is primarily a pitcher or hitter based on their stats"""
    pitching_query = """
    SELECT COUNT(*) as pitch_seasons, SUM(g) as total_games_pitched, SUM(gs) as total_starts
    FROM lahman_pitching WHERE playerid = %s
    """
    cursor = conn.cursor()
    cursor.execute(pitching_query, (playerid,))
    pitch_result = cursor.fetchone()

    batting_query = """
    SELECT COUNT(*) as bat_seasons, SUM(g) as total_games_batted, SUM(ab) as total_at_bats
    FROM lahman_batting WHERE playerid = %s
    """
    cursor.execute(batting_query, (playerid,))
    bat_result = cursor.fetchone()

    pitch_seasons = pitch_result[0] if pitch_result else 0
    total_games_pitched = pitch_result[1] if pitch_result and pitch_result[1] else 0
    total_starts = pitch_result[2] if pitch_result and pitch_result[2] else 0

    bat_seasons = bat_result[0] if bat_result else 0
    total_games_batted = bat_result[1] if bat_result and bat_result[1] else 0
    total_at_bats = bat_result[2] if bat_result and bat_result[2] else 0

    if pitch_seasons >= 3 or total_games_pitched >= 50 or total_starts >= 10:
        return "pitcher"
    elif bat_seasons >= 3 or total_at_bats >= 300:
        return "hitter"
    else:
        return "pitcher" if pitch_seasons > 0 else "hitter"


def get_player_awards(playerid, conn):
    """Get all awards for a player from the lahman database"""
    try:
        cursor = conn.cursor()

        # Query for all awards
        awards_query = """
        SELECT yearid, awardid, lgid, tie, notes
        FROM lahman_awardsplayers 
        WHERE playerid = %s
        ORDER BY yearid DESC, awardid
        """

        cursor.execute(awards_query, (playerid,))
        awards_data = cursor.fetchall()

        awards = []
        for row in awards_data:
            year, award_id, league, tie, notes = row

            # Format award name for display
            award_display = format_award_name(award_id)

            award_info = {
                "year": year,
                "award": award_display,
                "award_id": award_id,
                "league": league,
                "tie": bool(tie) if tie else False,
                "notes": notes,
            }
            awards.append(award_info)

        # Group and summarize awards
        award_summary = summarize_awards(awards)

        # Get MLB All-Star Game appearances:
        allstar_games = get_allstar_appearances(playerid, conn)

        # Get world series championships
        ws_championships = get_world_series_championships(playerid, conn)

        return {
            "awards": awards,
            "summary": award_summary,
            "mlbAllStar": allstar_games,
            "world_series_championships": ws_championships,
            "ws_count": len(ws_championships),
        }

    except Exception as e:
        return {
            "awards": [],
            "summary": {},
            "mlbAllStar": [],
            "world_series_championships": [],
            "ws_count": 0,
        }


def format_award_name(award_id):
    """Convert award IDs to readable names"""
    award_names = {
        "MVP": "Most Valuable Player",
        "CYA": "Cy Young Award",
        "CY": "Cy Young Award",
        "ROY": "Rookie of the Year",
        "GG": "Gold Glove",
        "SS": "Silver Slugger",
        "AS": "TSN All-Star Team",
        "WSMVP": "World Series MVP",
        "WS": "World Series Champion",
        "ALCS MVP": "ALCS MVP",
        "NLCS MVP": "NLCS MVP",
        "ASG MVP": "All-Star Game MVP",
        "ASGMVP": "All-Star Game MVP",
        "COMEB": "Comeback Player of the Year",
        "Hutch": "Hutch Award",
        "Lou Gehrig": "Lou Gehrig Memorial Award",
        "Babe Ruth": "Babe Ruth Award",
        "Roberto Clemente": "Roberto Clemente Award",
        "Branch Rickey": "Branch Rickey Award",
        "Hank Aaron": "Hank Aaron Award",
        "DHL Hometown Hero": "DHL Hometown Hero",
        "Edgar Martinez": "Edgar Martinez Outstanding DH Award",
        "Hutch Award": "Hutch Award",
        "Man of the Year": "Man of the Year",
        "Players Choice": "Players Choice Award",
        "Reliever": "Reliever of the Year",
        "TSN Fireman": "The Sporting News Fireman Award",
        "TSN MVP": "The Sporting News MVP",
        "TSN Pitcher": "The Sporting News Pitcher of the Year",
        "TSN Player": "The Sporting News Player of the Year",
        "TSN Rookie": "The Sporting News Rookie of the Year",
    }

    return award_names.get(award_id, award_id)


def summarize_awards(awards):
    """Create summary statistics for awards"""
    summary = {}

    # Count by award type
    for award in awards:
        award_id = award["award_id"]
        if award_id not in summary:
            summary[award_id] = {
                "count": 0,
                "years": [],
                "display_name": award["award"],
            }
        summary[award_id]["count"] += 1
        summary[award_id]["years"].append(award["year"])

    # Sort years for each award
    for award_id in summary:
        summary[award_id]["years"].sort(reverse=True)

    return summary


def get_allstar_appearances(playerid, conn):
    """Get MLB All-Star Game appearances from AllstarFull table"""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*) as allstar_games
            FROM lahman_allstarfull 
            WHERE playerid = %s
        """,
            (playerid,),
        )
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        return 0


# Add this new route for two-way player handling
@app.route("/player-two-way")
def get_player_with_two_way():
    """Enhanced player endpoint that handles two-way players"""
    name = request.args.get("name", "")
    mode = request.args.get("mode", "career").lower()
    player_type = request.args.get("player_type", "").lower()  # "pitcher" or "hitter"

    if " " not in name:
        return jsonify({"error": "Enter full name"}), 400

    # Try improved lookup with disambiguation
    playerid, suggestions = improved_player_lookup_with_disambiguation(name)

    if playerid is None and suggestions:
        return (
            jsonify(
                {
                    "error": "Multiple players found",
                    "suggestions": suggestions,
                    "message": f"Found {len(suggestions)} players named '{name.split(' Jr.')[0].split(' Sr.')[0]}'. Please specify which player:",
                }
            ),
            422,
        )

    if playerid is None:
        return jsonify({"error": "Player not found"}), 404

    conn = get_db_connection()
    detected_type = detect_two_way_player_simple(playerid, conn)

    # Get player's actual name for display
    cursor = conn.cursor()
    cursor.execute(
        "SELECT namefirst, namelast FROM lahman_people WHERE playerid = %s", (playerid,)
    )
    name_result = cursor.fetchone()
    first, last = name_result if name_result else ("Unknown", "Unknown")

    # Handle two-way players
    if detected_type == "two-way" and not player_type:
        # Return options for user to choose
        conn.close()
        return (
            jsonify(
                {
                    "error": "Two-way player detected",
                    "player_type": "two-way",
                    "options": [
                        {
                            "type": "pitcher",
                            "label": f"{first} {last} (Pitching Stats)",
                        },
                        {"type": "hitter", "label": f"{first} {last} (Hitting Stats)"},
                    ],
                    "message": f"{first} {last} is a known two-way player. Please select which stats to display:",
                }
            ),
            423,
        )  # Using 423 for two-way player selection

    # Use specified player_type or detected type
    final_type = player_type if player_type in ["pitcher", "hitter"] else detected_type
    if final_type == "two-way":
        final_type = "hitter"  # Default fallback

    # Get photo URL
    photo_url = get_photo_url_for_player(playerid, conn)

    # Process stats based on final type
    if final_type == "pitcher":
        return handle_pitcher_stats(playerid, conn, mode, photo_url, first, last)
    else:
        conn.close()
        return handle_hitter_stats(playerid, mode, photo_url, first, last)


@app.route("/search-players")
def search_players_enhanced():
    """Enhanced search that handles father/son players and provides disambiguation"""
    query = request.args.get("q", "").strip()

    if len(query) < 2:
        return jsonify([])

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query_clean = query.lower().strip()
        search_term = f"%{query_clean}%"

        # Enhanced search with birth year and additional info for disambiguation
        search_query = """
        SELECT DISTINCT 
            p.namefirst || ' ' || p.namelast as full_name,
            p.playerid,
            p.debut,
            p.finalgame,
            p.birthyear,
            p.birthmonth,
            p.birthday,
            CASE 
                WHEN LOWER(p.namefirst || ' ' || p.namelast) LIKE %s THEN 1
                WHEN LOWER(p.namelast) LIKE %s THEN 2
                WHEN LOWER(p.namefirst) LIKE %s THEN 3
                ELSE 4
            END as priority,
            -- Get primary position
            (SELECT pos FROM lahman_fielding f 
             WHERE f.playerid = p.playerid 
             GROUP BY pos 
             ORDER BY SUM(g) DESC 
             LIMIT 1) as primary_pos,
            -- Check if this player has stats (to filter out coaches, etc.)
            CASE WHEN EXISTS (
                SELECT 1 FROM lahman_batting b WHERE b.playerid = p.playerid
            ) OR EXISTS (
                SELECT 1 FROM lahman_pitching pt WHERE pt.playerid = p.playerid
            ) THEN 1 ELSE 0 END as has_stats
        FROM lahman_people p
        WHERE (LOWER(p.namefirst || ' ' || p.namelast) LIKE %s
           OR LOWER(p.namelast) LIKE %s
           OR LOWER(p.namefirst) LIKE %s)
        AND p.birthyear IS NOT NULL  -- Filter out entries without birth year
        ORDER BY priority, has_stats DESC, p.debut DESC, p.namelast, p.namefirst
        LIMIT 15
        """

        cursor.execute(
            search_query,
            (
                f"{query_clean}%",
                f"{query_clean}%",
                f"{query_clean}%",
                search_term,
                search_term,
                search_term,
            ),
        )

        results = cursor.fetchall()

        # Group players by name to detect duplicates
        name_groups = {}
        for row in results:
            (
                full_name,
                playerid,
                debut,
                final_game,
                birth_year,
                birth_month,
                birth_day,
                priority,
                position,
                has_stats,
            ) = row

            if full_name not in name_groups:
                name_groups[full_name] = []

            name_groups[full_name].append(
                {
                    "full_name": full_name,
                    "playerid": playerid,
                    "debut": debut,
                    "final_game": final_game,
                    "birth_year": birth_year,
                    "birth_month": birth_month,
                    "birth_day": birth_day,
                    "position": position,
                    "has_stats": has_stats,
                }
            )

        conn.close()

        # Process results and add disambiguation
        players = []
        for name, player_list in name_groups.items():
            if len(player_list) == 1:
                # Single player with this name
                player = player_list[0]
                debut_year = player["debut"][:4] if player["debut"] else "Unknown"

                if player["position"]:
                    display_name = f"{name} ({player['position']}, {debut_year})"
                else:
                    display_name = f"{name} ({debut_year})"

                players.append(
                    {
                        "name": name,
                        "display": display_name,
                        "playerid": player["playerid"],
                        "debut_year": debut_year,
                        "position": player["position"] or "Unknown",
                        "disambiguation": None,
                    }
                )
            else:
                # Multiple players with same name - add disambiguation
                # Sort by debut year (older first)
                player_list.sort(key=lambda x: x["debut"] or "9999")

                for i, player in enumerate(player_list):
                    debut_year = player["debut"][:4] if player["debut"] else "Unknown"
                    birth_year = player["birth_year"] or "Unknown"

                    # Determine suffix (Sr./Jr. or I/II based on debut order)
                    if len(player_list) == 2:
                        suffix = "Sr." if i == 0 else "Jr."
                    else:
                        suffix = ["Sr.", "Jr.", "III"][i] if i < 3 else f"({i+1})"

                    # Create display name with disambiguation
                    base_display = f"{name} {suffix}"
                    if player["position"]:
                        display_name = (
                            f"{base_display} ({player['position']}, {debut_year})"
                        )
                    else:
                        display_name = f"{base_display} ({debut_year})"

                    players.append(
                        {
                            "name": name,
                            "display": display_name,
                            "playerid": player["playerid"],
                            "debut_year": debut_year,
                            "birth_year": str(birth_year),
                            "position": player["position"] or "Unknown",
                            "disambiguation": suffix,
                            "original_name": name,
                        }
                    )

        return jsonify(players)

    except Exception as e:
        return jsonify([])


def improved_player_lookup_with_disambiguation(name):
    """
    Improved player lookup that handles common father/son cases
    and provides suggestions when multiple players exist
    """

    # Handle common suffixes
    suffixes = {
        "jr": "Jr.",
        "jr.": "Jr.",
        "junior": "Jr.",
        "sr": "Sr.",
        "sr.": "Sr.",
        "senior": "Sr.",
        "ii": "II",
        "iii": "III",
        "2nd": "II",
        "3rd": "III",
    }

    name_lower = name.lower().strip()
    suffix = None
    clean_name = name

    # Check if name contains a suffix
    for suffix_variant, standard_suffix in suffixes.items():
        if name_lower.endswith(" " + suffix_variant):
            suffix = standard_suffix
            clean_name = name[: -(len(suffix_variant) + 1)].strip()
            break

    if " " not in clean_name:
        return None, []

    first, last = clean_name.split(" ", 1)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Find all players with this name
    all_players_query = """
    SELECT playerid, namefirst, namelast, debut, finalgame, birthyear
    FROM lahman_people
    WHERE LOWER(namefirst) = %s AND LOWER(namelast) = %s
    ORDER BY debut
    """

    cursor.execute(all_players_query, (first.lower(), last.lower()))
    all_matches = cursor.fetchall()
    conn.close()

    if not all_matches:
        return None, []

    if len(all_matches) == 1:
        return all_matches[0][0], []

    # Multiple players found
    suggestions = []
    target_player = None

    for i, (playerid, fname, lname, debut, final_game, birth_year) in enumerate(
        all_matches
    ):
        full_name = f"{fname} {lname}"
        debut_year = debut[:4] if debut else "Unknown"

        # Create suggestion with disambiguation
        if len(all_matches) == 2:
            player_suffix = "Sr." if i == 0 else "Jr."
        else:
            player_suffix = ["Sr.", "Jr.", "III"][i] if i < 3 else f"({i+1})"

        suggestion = {
            "name": f"{full_name} {player_suffix}",
            "playerid": playerid,
            "debut_year": debut_year,
            "birth_year": birth_year or "Unknown",
        }
        suggestions.append(suggestion)

        # If user specified a suffix, try to match it
        if suffix:
            if suffix == player_suffix:
                target_player = playerid

    return target_player, suggestions


@app.route("/player-disambiguate")
def get_player_with_disambiguation():
    """Enhanced player endpoint that handles disambiguation"""
    name = request.args.get("name", "")
    mode = request.args.get("mode", "career").lower()
    player_type = request.args.get("player_type", "").lower()

    if " " not in name:
        return jsonify({"error": "Enter full name"}), 400

    # Try improved lookup
    playerid, suggestions = improved_player_lookup_with_disambiguation(name)

    if playerid is None and suggestions:
        # Multiple players found, return suggestions
        return (
            jsonify(
                {
                    "error": "Multiple players found",
                    "suggestions": suggestions,
                    "message": f"Found {len(suggestions)} players named '{name.split(' Jr.')[0].split(' Sr.')[0]}'. Please specify which player:",
                }
            ),
            422,
        )

    if playerid is None:
        return jsonify({"error": "Player not found"}), 404

    # Continue with existing logic using the found playerid
    conn = get_db_connection()
    detected_type = detect_two_way_player_simple(playerid, conn)

    # Get player's actual name for display
    cursor = conn.cursor()
    cursor.execute(
        "SELECT namefirst, namelast FROM lahman_people WHERE playerid = %s", (playerid,)
    )
    name_result = cursor.fetchone()
    first, last = name_result if name_result else ("Unknown", "Unknown")

    # Handle two-way players
    if detected_type == "two-way" and not player_type:
        conn.close()
        return (
            jsonify(
                {
                    "error": "Two-way player detected",
                    "player_type": "two-way",
                    "options": [
                        {
                            "type": "pitcher",
                            "label": f"{first} {last} (Pitching Stats)",
                        },
                        {"type": "hitter", "label": f"{first} {last} (Hitting Stats)"},
                    ],
                    "message": f"{first} {last} is a known two-way player. Please select which stats to display:",
                }
            ),
            423,
        )

    # Continue with existing logic using specified or detected type
    final_type = player_type if player_type in ["pitcher", "hitter"] else detected_type
    if final_type == "two-way":
        final_type = "hitter"  # Default fallback
    # Rest of the logic remains the same...
    # [Include your existing photo URL and stats logic here]
    photo_url = get_photo_url_for_player(playerid, conn)

    if final_type == "pitcher":
        return handle_pitcher_stats(playerid, conn, mode, photo_url, first, last)
    else:
        conn.close()
        return handle_hitter_stats(playerid, mode, photo_url, first, last)


@app.route("/popular-players")
def popular_players():
    fallback_players = [
        "Mike Trout",
        "Aaron Judge",
        "Mookie Betts",
        "Ronald Acuña",
        "Juan Soto",
        "Vladimir Guerrero Jr.",
        "Fernando Tatis Jr.",
        "Gerrit Cole",
        "Jacob deGrom",
        "Tarik Skubal",
        "Spencer Strider",
        "Freddie Freeman",
        "Manny Machado",
        "Jose Altuve",
        "Kyle Tucker",
    ]
    return jsonify(fallback_players)


# Optional: Add a route to get all unique player names (for advanced frontend caching)
@app.route("/all-players")
def all_players():
    """Get all player names - useful for client-side caching if needed"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all players who have batting or pitching stats
        all_players_query = """
        SELECT DISTINCT p.namefirst || ' ' || p.namelast as full_name
        FROM lahman_people p
        WHERE EXISTS (
            SELECT 1 FROM lahman_batting b WHERE b.playerid = p.playerid
        ) OR EXISTS (
            SELECT 1 FROM lahman_pitching pt WHERE pt.playerid = p.playerid
        )
        ORDER BY p.namelast, p.namefirst
        """

        cursor.execute(all_players_query)
        results = cursor.fetchall()
        conn.close()

        players = [row[0] for row in results]
        return jsonify(players)

    except Exception as e:
        return jsonify([])


# Also add this helper function to improve your existing player lookup
def improved_player_lookup(name):
    """Improved player lookup with better fuzzy matching"""
    if " " not in name:
        return None

    first, last = name.split(" ", 1)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Try exact match first
    exact_query = """
    SELECT playerid FROM lahman_people
    WHERE LOWER(namefirst) = %s AND LOWER(namelast) = %s
    LIMIT 1
    """
    cursor.execute(exact_query, (first.lower(), last.lower()))
    result = cursor.fetchone()

    if result:
        conn.close()
        return result[0]

    # Try fuzzy matching
    fuzzy_query = """
    SELECT playerid, namefirst, namelast,
           CASE 
               WHEN LOWER(namelast) = %s THEN 1
               WHEN LOWER(namefirst) = %s THEN 2  
               WHEN LOWER(namelast) LIKE %s THEN 3
               WHEN LOWER(namefirst) LIKE %s THEN 4
               ELSE 5
           END as match_quality
    FROM lahman_people
    WHERE LOWER(namelast) LIKE %s OR LOWER(namefirst) LIKE %s
    ORDER BY match_quality
    LIMIT 1
    """

    search_pattern = f"%{last.lower()}%"
    first_pattern = f"%{first.lower()}%"

    cursor.execute(
        fuzzy_query,
        (
            last.lower(),
            first.lower(),
            search_pattern,
            first_pattern,
            search_pattern,
            first_pattern,
        ),
    )

    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


@app.route("/")
def serve_index():
    return send_from_directory("static", "index.html")


@app.route("/player")
def get_player_stats():
    name = request.args.get("name", "")
    mode = request.args.get("mode", "career").lower()

    if " " not in name:
        return jsonify({"error": "Enter full name"}), 400

    first, last = name.split(" ", 1)

    conn = get_db_connection()
    query_id = """
    SELECT playerid FROM lahman_people
    WHERE LOWER(namefirst) = %s AND LOWER(namelast) = %s
    LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(query_id, (first.lower(), last.lower()))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Player not found"}), 404

    playerid = row[0]

    player_type = detect_player_type(playerid, conn)

    # get player photo
    photo_url = get_photo_url_for_player(playerid, conn)

    if player_type == "pitcher":
        return handle_pitcher_stats(playerid, conn, mode, photo_url, first, last)
    else:
        conn.close()
        return handle_hitter_stats(playerid, mode, photo_url, first, last)


def handle_pitcher_stats(playerid, conn, mode, photo_url, first, last):
    # Remove all live stats fetching
    stats_query = """
    SELECT yearid, teamid, w, l, g, gs, cg, sho, sv, ipouts, h, er, hr, bb, so, era
    FROM lahman_pitching WHERE playerid = %s
    """
    df_lahman = pd.read_sql_query(stats_query, conn, params=(playerid,))

    # get awards for this player
    awards_data = get_player_awards(playerid, conn)
    
    if mode == "career":
        if df_lahman.empty:
            return jsonify({"error": "No pitching stats found"}), 404

        totals = df_lahman.agg({
            "w": "sum", "l": "sum", "g": "sum", "gs": "sum", "cg": "sum", 
            "sho": "sum", "sv": "sum", "ipouts": "sum", "h": "sum", 
            "er": "sum", "hr": "sum", "bb": "sum", "so": "sum",
        }).to_dict()

        innings_pitched = totals["ipouts"] / 3.0 if totals["ipouts"] > 0 else 0
        era = (totals["er"] * 9) / innings_pitched if innings_pitched > 0 else 0
        whip = (totals["h"] + totals["bb"]) / innings_pitched if innings_pitched > 0 else 0
        career_war = get_career_war(playerid)

        result = {
            "war": round(career_war, 1),
            "wins": int(totals["w"]),
            "losses": int(totals["l"]),
            "games": int(totals["g"]),
            "games_started": int(totals["gs"]),
            "complete_games": int(totals["cg"]),
            "shutouts": int(totals["sho"]),
            "saves": int(totals["sv"]),
            "innings_pitched": round(innings_pitched, 1),
            "hits_allowed": int(totals["h"]),
            "earned_runs": int(totals["er"]),
            "home_runs_allowed": int(totals["hr"]),
            "walks": int(totals["bb"]),
            "strikeouts": int(totals["so"]),
            "era": round(era, 2),
            "whip": round(whip, 2),
        }

        conn.close()
        return jsonify({
            "mode": "career",
            "player_type": "pitcher", 
            "totals": result,
            "photo_url": photo_url,
            "awards": awards_data,
        })

    elif mode == "season":
        if df_lahman.empty:
            return jsonify({"error": "No pitching stats found"}), 404

        df_war_history = get_season_war_history(playerid)

        df = df_lahman.copy()
        df["innings_pitched"] = df["ipouts"] / 3.0
        df["era_calc"] = df.apply(
            lambda row: (row["er"] * 9) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0,
            axis=1,
        )
        df["whip"] = df.apply(
            lambda row: (row["h"] + row["bb"]) / (row["ipouts"] / 3.0) if row["ipouts"] > 0 else 0,
            axis=1,
        )
        df["era_final"] = df.apply(
            lambda row: row["era"] if row["era"] > 0 else row["era_calc"], axis=1
        )

        if not df_war_history.empty:
            df = df.merge(df_war_history, left_on="yearid", right_on="year_ID", how="left")
            df["war"] = df["war"].fillna(0)
        else:
            df["war"] = 0

        df_result = df[[
            "yearid", "teamid", "w", "l", "g", "gs", "cg", "sho", "sv",
            "innings_pitched", "h", "er", "hr", "bb", "so", "era_final", "whip", "war",
        ]].rename(columns={
            "yearid": "year", "w": "wins", "l": "losses", "g": "games",
            "gs": "games_started", "cg": "complete_games", "sho": "shutouts",
            "sv": "saves", "h": "hits_allowed", "er": "earned_runs",
            "hr": "home_runs_allowed", "bb": "walks", "so": "strikeouts",
            "era_final": "era",
        })

        conn.close()
        return jsonify({
            "mode": "season",
            "player_type": "pitcher",
            "stats": df_result.to_dict(orient="records"),
            "photo_url": photo_url,
            "awards": awards_data,
        })

    # Return error for live and combined modes
    elif mode in ["live", "combined"]:
        conn.close()
        return jsonify({"error": f"{mode.title()} stats temporarily disabled"}), 503

    else:
        conn.close()
        return jsonify({"error": "Invalid mode"}), 400


def handle_hitter_stats(playerid, mode, photo_url, first, last):
    conn = get_db_connection()
    
    stats_query = """
    SELECT yearid, teamid, g, ab, h, hr, rbi, sb, bb, hbp, sf, sh, "2b", "3b"
    FROM lahman_batting WHERE playerid = %s
    """
    df_lahman = pd.read_sql_query(stats_query, conn, params=(playerid,))
    awards_data = get_player_awards(playerid, conn)
    
    if mode == "career":
        if df_lahman.empty:
            conn.close()
            return jsonify({"error": "No batting stats found"}), 404

        totals = df_lahman.agg({
            "g": "sum", "ab": "sum", "h": "sum", "hr": "sum", "rbi": "sum",
            "sb": "sum", "bb": "sum", "hbp": "sum", "sf": "sum", "sh": "sum",
            "2b": "sum", "3b": "sum",
        }).to_dict()

        singles = totals["h"] - totals["2b"] - totals["3b"] - totals["hr"]
        total_bases = singles + 2 * totals["2b"] + 3 * totals["3b"] + 4 * totals["hr"]
        ba = totals["h"] / totals["ab"] if totals["ab"] > 0 else 0
        obp_denominator = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"]
        obp = (totals["h"] + totals["bb"] + totals["hbp"]) / obp_denominator if obp_denominator > 0 else 0
        slg = total_bases / totals["ab"] if totals["ab"] > 0 else 0
        ops = obp + slg
        plate_appearances = totals["ab"] + totals["bb"] + totals["hbp"] + totals["sf"] + totals["sh"]
        career_war = get_career_war(playerid)

        result = {
            "war": round(career_war, 1),
            "games": int(totals["g"]),
            "plate_appearances": int(plate_appearances),
            "hits": int(totals["h"]),
            "home_runs": int(totals["hr"]),
            "rbi": int(totals["rbi"]),
            "stolen_bases": int(totals["sb"]),
            "batting_average": round(ba, 3),
            "on_base_percentage": round(obp, 3),
            "slugging_percentage": round(slg, 3),
            "ops": round(ops, 3),
            "ops_plus": 0,  # Remove live stats dependency
        }

        conn.close()
        return jsonify({
            "mode": "career",
            "player_type": "hitter",
            "totals": result,
            "photo_url": photo_url,
            "awards": awards_data,
        })

    elif mode == "season":
        if df_lahman.empty:
            conn.close()
            return jsonify({"error": "No batting stats found"}), 404

        df_war_history = get_season_war_history(playerid)

        df = df_lahman.copy()
        df["singles"] = df["h"] - df["2b"] - df["3b"] - df["hr"]
        df["total_bases"] = df["singles"] + 2 * df["2b"] + 3 * df["3b"] + 4 * df["hr"]
        df["ba"] = df.apply(lambda row: row["h"] / row["ab"] if row["ab"] > 0 else 0, axis=1)
        df["obp"] = df.apply(lambda row: (
            (row["h"] + row["bb"] + row["hbp"]) / 
            (row["ab"] + row["bb"] + row["hbp"] + row["sf"])
            if (row["ab"] + row["bb"] + row["hbp"] + row["sf"]) > 0 else 0
        ), axis=1)
        df["slg"] = df.apply(lambda row: row["total_bases"] / row["ab"] if row["ab"] > 0 else 0, axis=1)
        df["ops"] = df["obp"] + df["slg"]
        df["pa"] = df["ab"] + df["bb"] + df["hbp"] + df["sf"] + df["sh"]

        if not df_war_history.empty:
            df = df.merge(df_war_history, left_on="yearid", right_on="year_ID", how="left")
            df["war"] = df["war"].fillna(0)
        else:
            df["war"] = 0

        df_result = df[[
            "yearid", "teamid", "g", "pa", "ab", "h", "hr", "rbi", "sb", "bb",
            "hbp", "sf", "2b", "3b", "ba", "obp", "slg", "ops", "war",
        ]].rename(columns={
            "yearid": "year", "g": "games", "ab": "at_bats", "h": "hits",
            "hr": "home_runs", "rbi": "rbi", "sb": "stolen_bases", "bb": "walks",
            "hbp": "hit_by_pitch", "sf": "sacrifice_flies", "2b": "doubles", "3b": "triples",
        })

        conn.close()
        return jsonify({
            "mode": "season",
            "player_type": "hitter",
            "stats": df_result.to_dict(orient="records"),
            "photo_url": photo_url,
            "awards": awards_data,
        })

    # Return error for live and combined modes
    elif mode in ["live", "combined"]:
        conn.close()
        return jsonify({"error": f"{mode.title()} stats temporarily disabled"}), 503

    else:
        conn.close()
        return jsonify({"error": "Invalid mode"}), 400

'''
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
        conn = get_db_connection()

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
            WHERE teamid = %s AND yearid = %s
            """
            df = pd.read_sql_query(query, conn, params=(team_id, actual_year))

        elif mode in ["franchise", "career", "overall"]:

            # Check for franchise moves - Milwaukee Brewers example
            franchise_ids = get_franchise_team_ids(team_id)

            if len(franchise_ids) > 1:
                # Multiple team IDs for this franchise
                placeholders = ",".join(["%s" for _ in franchise_ids])
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
                WHERE teamid = %s
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
            WHERE teamid = %s AND yearid = %s
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
            WHERE (teamidwinner = %s OR teamidloser = %s) 
            AND yearid = %s
            """

            playoff_df = pd.read_sql_query(
                playoff_query, conn, params=(team_id, team_id, actual_year)
            )
            playoff_apps = 1 if playoff_df.iloc[0]["series_count"] > 0 else 0

            # Check World Series appearances
            ws_query = """
            SELECT COUNT(*) as ws_series
            FROM lahman_seriespost 
            WHERE (teamidwinner = %s OR teamidloser = %s) 
            AND yearid = %s 
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
            WHERE teamidwinner = %s
            AND yearid = %s 
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
            WHERE (teamidwinner = %s OR teamidloser = %s)
            """

            playoff_df = pd.read_sql_query(
                playoff_query, conn, params=(team_id, team_id)
            )
            playoff_apps = playoff_df.iloc[0]["playoff_years"]

            # Count World Series appearances
            ws_query = """
            SELECT COUNT(DISTINCT yearid) as ws_years
            FROM lahman_seriespost 
            WHERE (teamidwinner = %s OR teamidloser = %s) 
            AND round = 'WS'
            """

            ws_df = pd.read_sql_query(ws_query, conn, params=(team_id, team_id))
            ws_apps = ws_df.iloc[0]["ws_years"]

            # Count World Series championships
            ws_win_query = """
            SELECT COUNT(*) as total_ws_wins
            FROM lahman_seriespost 
            WHERE teamidwinner = %s
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
        conn = get_db_connection()

        # Get regular season head-to-head from Retrosheet teamstats
        regular_season_record = get_regular_season_h2h(
            conn, team_a, team_b, year_filter
        )

        # Get playoff data (your existing code)
        playoff_query = """
        SELECT yearid, round, teamidwinner, teamidloser, wins, losses
        FROM lahman_seriespost 
        WHERE (
            (teamidwinner = %s AND teamidloser = %s) OR 
            (teamidwinner = %s AND teamidloser = %s)
        )
        """

        params = [team_a, team_b, team_b, team_a]

        if year_filter:
            playoff_query += " AND yearid = %s"
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
        team_a_placeholders = ",".join(["%s" for _ in team_a_ids])
        team_b_placeholders = ",".join(["%s" for _ in team_b_ids])

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
            base_query += f" AND substr({year_col}, 1, 4) = %s"
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