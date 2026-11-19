"""Read-only: latest game_qa row per game that is not 'clear', with its failing gates."""
import sys
from pathlib import Path

from euroleague_possessions.postgres_backend import connect_from_env_file

conn = connect_from_env_file(Path(sys.argv[1]))
cur = conn.cursor()
cur.execute("BEGIN READ ONLY")
cur.execute(
    """
    WITH latest AS (
      SELECT DISTINCT ON (game_id) *
        FROM euroleague.game_qa
       ORDER BY game_id, load_run_id DESC
    )
    SELECT s.competition, s.season, s.gamecode, l.publication_status,
           l.possession_structural_status, l.possession_review_status,
           l.boxscore_metrics_exact, l.score_progression_exact, l.score_progression_reconciled,
           l.lineup_structure_valid, l.lineup_invalid_actor_rows,
           l.same_team_transitions, l.provisional_ft_rows, l.unresolved_ft_rows
      FROM latest l JOIN euroleague.schedule s USING (game_id)
     ORDER BY l.publication_status, s.competition, s.season, s.gamecode
    """
)
rows = cur.fetchall()
cur.execute("ROLLBACK")
from collections import Counter
print("status counts:", Counter(r[3] for r in rows))
print("comp season game status | struct review box_exact prog_exact prog_recon lineup_ok bad_actors same_team prov_ft unres_ft")
for r in rows:
    if r[3] != "clear":
        print(*r)
