"""Deterministic text summary generation for chess game reviews."""
from typing import Optional

BLUNDER_THRESH = 3.0    # pawns (cp_score is stored in pawns)
MISTAKE_THRESH = 1.5
INACCURACY_THRESH = 0.5

_PIECE_VALUES = {'P': 1, 'N': 3, 'B': 3, 'R': 5, 'Q': 9}


def _material_balance(fen: str) -> int:
    """White material minus black material from a FEN string (pawns = 1pt each)."""
    board = fen.split(' ')[0]
    balance = 0
    for ch in board:
        v = _PIECE_VALUES.get(ch.upper())
        if v:
            balance += v if ch.isupper() else -v
    return balance


def _material_desc(pts: int) -> str:
    """Human-readable description of a material advantage in points."""
    if pts == 0:
        return "equal material"
    sign = "up" if pts > 0 else "down"
    abs_pts = abs(pts)
    if abs_pts == 1:
        label = "a pawn"
    elif abs_pts == 2:
        label = "2 pawns"
    elif abs_pts == 3:
        label = "a minor piece"
    elif abs_pts == 4:
        label = "a minor piece + pawn"
    elif abs_pts == 5:
        label = "a rook"
    elif abs_pts == 6:
        label = "a rook + pawn"
    elif abs_pts == 7:
        label = "a rook + 2 pawns"
    elif abs_pts == 8:
        label = "rook + minor piece"
    elif abs_pts == 9:
        label = "a queen"
    elif abs_pts <= 11:
        label = "queen + pawns"
    else:
        label = f"{abs_pts} points"
    return f"{sign} {label} ({'+' if pts > 0 else ''}{pts})"


def _phase(move_number: int) -> str:
    if move_number <= 20:
        return "opening"
    if move_number <= 60:
        return "middlegame"
    return "endgame"


def _sign(n: float) -> str:
    if n == 0 or n == -0.0:
        return "+0.00"
    return f"+{n:.2f}" if n > 0 else f"{n:.2f}"


def _compute_drops(eval_map: dict, for_white: bool) -> list:
    """Return all drops >= INACCURACY_THRESH for one side."""
    drops = []
    for mn in sorted(eval_map.keys()):
        if mn == 0:
            continue
        prev_mn = mn - 1
        if prev_mn not in eval_map:
            continue
        prev_cp = eval_map[prev_mn]
        curr_cp = eval_map[mn]
        this_move_white = (mn % 2 == 1)
        if this_move_white != for_white:
            continue
        drop = (prev_cp - curr_cp) if for_white else (curr_cp - prev_cp)
        if drop >= INACCURACY_THRESH:
            drops.append({
                "move": mn,
                "drop": drop,
                "before": prev_cp,
                "after": curr_cp,
                "phase": _phase(mn),
            })
    return drops


def _termination_method(game: dict, evals: list) -> str:
    """
    Return a short phrase describing how the game ended, e.g.
    'by checkmate', 'by resignation', 'on time', 'by abandonment'.
    """
    termination = (game.get("termination") or "").lower()
    result = game.get("result", "?")

    if result == "1/2-1/2":
        if "time" in termination:
            return "by time forfeit"
        return ""   # draw — caller handles label

    if "time" in termination:
        return "on time"
    if "rules" in termination or "infraction" in termination or "abandon" in termination:
        return "by abandonment"

    # "Normal" termination — checkmate or resignation
    # Check whether the last eval entry had a forced mate signal
    if evals:
        last_eval = evals[-1]
        if last_eval.get("mate_in") is not None:
            return "by checkmate"
    return "by resignation"


def _result_headline(game: dict, is_white: bool, outcome: str, evals: list) -> str:
    """
    Build the first line of the review, e.g.
    'Win vs Opponent (1500) — luckleland won by checkmate · Opening · 42 moves'
    """
    opponent = game.get("black" if is_white else "white") or "?"
    opp_elo  = game.get("black_elo" if is_white else "white_elo")
    opening  = game.get("opening") or "Unknown opening"
    result   = game.get("result", "?")

    elo_str  = f" ({opp_elo})" if opp_elo else ""
    method   = _termination_method(game, evals)

    if outcome == "Draw":
        if method:
            how = f" — Draw {method}"
        else:
            how = " — Draw"
    else:
        winner_name = game.get("white") if result == "1-0" else game.get("black")
        how = f" — {winner_name} won {method}" if method else ""

    return f"{outcome} vs {opponent}{elo_str}{how} · {opening}"


def _material_duration_stats(user_balances: list) -> dict:
    """
    Given a list of (move_number, user_balance) tuples, return:
      ahead_pct, behind_pct, avg_balance, max_ahead_streak (in half-moves)
    """
    if not user_balances:
        return {}
    total = len(user_balances)
    balances = [b for _, b in user_balances]
    ahead_count  = sum(1 for b in balances if b > 0)
    behind_count = sum(1 for b in balances if b < 0)
    avg_balance  = sum(balances) / total

    max_streak = cur_streak = 0
    for b in balances:
        if b > 0:
            cur_streak += 1
            max_streak = max(max_streak, cur_streak)
        else:
            cur_streak = 0

    return {
        "ahead_pct":   round(ahead_count  / total * 100),
        "behind_pct":  round(behind_count / total * 100),
        "avg_balance": round(avg_balance, 1),
        "max_streak":  max_streak,   # half-moves
    }


def _material_after_blunder(blunder_mn: int, material_map: dict,
                             is_white: bool, sorted_mns: list,
                             window: int = 15) -> Optional[dict]:
    """
    Returns a dict with per-side counts and edge ranges for the `window`
    half-moves following a blunder:
      user_count / opp_count: half-moves each side held material advantage
      user_min_edge / user_max_edge: range of user's lead when they were ahead
      opp_min_edge / opp_max_edge: range of opponent's lead when they were ahead
      window: total half-moves examined
    Returns None if no subsequent data.
    """
    subsequent = [mn for mn in sorted_mns
                  if blunder_mn < mn <= blunder_mn + window]
    if not subsequent:
        return None
    user_balances = [(material_map[mn] if is_white else -material_map[mn])
                     for mn in subsequent]
    user_ahead = [b for b in user_balances if b > 0]
    opp_ahead  = [-b for b in user_balances if b < 0]
    return {
        "user_count":    len(user_ahead),
        "opp_count":     len(opp_ahead),
        "user_min_edge": min(user_ahead) if user_ahead else 0,
        "user_max_edge": max(user_ahead) if user_ahead else 0,
        "opp_min_edge":  min(opp_ahead)  if opp_ahead  else 0,
        "opp_max_edge":  max(opp_ahead)  if opp_ahead  else 0,
        "window":        len(subsequent),
    }


_PIECE_NAMES_FULL = {'P': 'pawn', 'N': 'knight', 'B': 'bishop', 'R': 'rook', 'Q': 'queen'}


def _count_pieces(fen: str) -> dict:
    board = fen.split(' ')[0]
    counts: dict = {}
    for ch in board:
        if ch.isalpha() and ch.upper() in 'PNBRQ':
            counts[ch] = counts.get(ch, 0) + 1
    return counts


def _pieces_lost(fen_a: str, fen_b: str) -> tuple:
    """
    Compare two FEN positions; return (white_lost, black_lost) —
    lists of piece-name strings. Accounts for pawn promotions.
    """
    before = _count_pieces(fen_a)
    after  = _count_pieces(fen_b)

    def removed(chars):
        result = []
        for ch in chars:
            delta = before.get(ch, 0) - after.get(ch, 0)
            if delta > 0:
                result.extend([_PIECE_NAMES_FULL[ch.upper()]] * delta)
        return result

    def added(chars):
        result = []
        for ch in chars:
            delta = after.get(ch, 0) - before.get(ch, 0)
            if delta > 0:
                result.extend([_PIECE_NAMES_FULL[ch.upper()]] * delta)
        return result

    white_lost = removed('PNBRQ')
    black_lost = removed('pnbrq')

    # Strip promoted pawns (a pawn disappears but a higher piece appears on same side)
    for wl, gained_chars in ((white_lost, 'NBRQ'), (black_lost, 'nbrq')):
        promos = min(wl.count('pawn'), len(added(gained_chars)))
        for _ in range(promos):
            wl.remove('pawn')

    return white_lost, black_lost


def _piece_list(pieces: list) -> str:
    """['knight', 'pawn'] → 'knight and pawn'"""
    if not pieces:
        return ""
    if len(pieces) == 1:
        return pieces[0]
    return ", ".join(pieces[:-1]) + " and " + pieces[-1]


def _game_story(
    game: dict,
    is_white: bool,
    total_moves: int,
    blunders: list,
    mistakes: list,
    opp_blunders: list,
    opp_mistakes: list,
    eval_map: dict,
    material_map: dict,
    evals: list,
) -> list:
    """Return the GAME STORY section as a list of text lines."""
    fen_map = {
        e["move_number"]: e["fen"]
        for e in evals
        if e.get("move_number") is not None and e.get("fen")
    }

    opponent  = game.get("black" if is_white else "white") or "opponent"
    result    = game.get("result", "?")
    if is_white:
        outcome = "Win" if result == "1-0" else ("Loss" if result == "0-1" else "Draw")
    else:
        outcome = "Win" if result == "0-1" else ("Loss" if result == "1-0" else "Draw")

    full_moves = (total_moves + 1) // 2
    opening    = game.get("opening") or "Unknown opening"

    parts = ["", "GAME STORY"]

    # Opening sentence
    early_eval, early_mn = None, None
    for mn in range(20, 5, -1):
        if mn in eval_map:
            early_eval, early_mn = eval_map[mn], mn
            break

    if early_eval is not None:
        ue = early_eval if is_white else -early_eval
        if ue >= 1.5:
            feel = f"you had a strong advantage (+{ue:.1f})"
        elif ue >= 0.3:
            feel = f"you held a slight edge (+{ue:.1f})"
        elif ue >= -0.3:
            feel = "the position was balanced"
        elif ue >= -1.5:
            feel = f"you were slightly worse ({ue:.1f})"
        else:
            feel = f"you were significantly worse ({ue:.1f})"
        parts.append(f"  {opening}. By move {(early_mn + 1) // 2}, {feel}.")
    else:
        parts.append(f"  {opening}.")

    # All significant events (blunders + mistakes) from both sides, in order
    events = (
        [(d["move"], True, d)  for d in blunders + mistakes] +
        [(d["move"], False, d) for d in opp_blunders + opp_mistakes]
    )
    events.sort(key=lambda x: x[0])

    if not events:
        parts.append(
            "  No major errors from either side — "
            "the game was decided by small, accumulated differences."
        )
    else:
        for mn, is_user_event, d in events:
            full_mn   = (mn + 1) // 2
            drop_cp   = int(round(d["drop"] * 100))
            err_label = "Blunder" if d["drop"] >= BLUNDER_THRESH else "Mistake"
            phase     = d["phase"]

            # Eval from user's perspective (positive = good for user)
            before_user = d["before"] if is_white else -d["before"]
            after_user  = d["after"]  if is_white else -d["after"]
            eval_str    = f"eval {_sign(before_user)} → {_sign(after_user)} ({drop_cp}cp)"

            # Detect pieces lost on this move AND the next (catches hanging-piece blunders)
            user_lost: list = []
            opp_lost:  list = []
            for check_mn in (mn, mn + 1):
                fa = fen_map.get(check_mn - 1)
                fb = fen_map.get(check_mn)
                if fa and fb:
                    wl, bl = _pieces_lost(fa, fb)
                    ul = wl if is_white else bl
                    ol = bl if is_white else wl
                    user_lost.extend(ul)
                    opp_lost.extend(ol)

            # Material balance right after the blunder move
            mat_str = ""
            if mn in material_map:
                user_mat = material_map[mn] if is_white else -material_map[mn]
                if user_mat > 0:
                    mat_str = f" You were up {user_mat}pt{'s' if user_mat != 1 else ''} in material."
                elif user_mat < 0:
                    mat_str = (
                        f" You were down {abs(user_mat)}pt{'s' if abs(user_mat) != 1 else ''} "
                        f"in material ({_material_desc(user_mat)})."
                    )
                else:
                    mat_str = " Material was equal."

            # Narrative for the capture / exchange
            if is_user_event:
                actor = f"Your {err_label.lower()}"
                if user_lost and opp_lost:
                    cap = (
                        f" You traded your {_piece_list(user_lost)} "
                        f"for {opponent}'s {_piece_list(opp_lost)}."
                    )
                elif user_lost:
                    cap = f" Your {_piece_list(user_lost)} was lost."
                elif opp_lost:
                    cap = f" You captured {opponent}'s {_piece_list(opp_lost)}."
                else:
                    cap = " No piece was immediately lost — a positional error."
            else:
                actor = f"{opponent}'s {err_label.lower()}"
                if opp_lost and user_lost:
                    cap = (
                        f" {opponent} traded their {_piece_list(opp_lost)} "
                        f"for your {_piece_list(user_lost)}."
                    )
                elif opp_lost:
                    cap = f" {opponent}'s {_piece_list(opp_lost)} was lost."
                elif user_lost:
                    cap = f" Your {_piece_list(user_lost)} was captured."
                else:
                    cap = " No piece was immediately lost — a positional error."

            parts.append(
                f"  Move {full_mn} ({phase}) — {actor}: {eval_str}.{cap}{mat_str}"
            )

    # Closing line
    term = _termination_method(game, evals)
    term_str = f" {term}" if term else ""
    if outcome == "Win":
        parts.append(f"  The game ended in your favor{term_str} on move {full_moves}.")
    elif outcome == "Loss":
        parts.append(f"  The game ended against you{term_str} on move {full_moves}.")
    else:
        parts.append(f"  The game ended in a draw on move {full_moves}.")

    return parts


def generate_review(game: dict, evals: list, username: str) -> dict:
    """
    Generate a deterministic review for a single game.

    Returns a dict with:
      blunder_count, mistake_count, inaccuracy_count,
      biggest_drop_cp (centipawns int),
      critical_move_number, critical_phase, text_summary
    """
    if not evals:
        return _empty_review()

    u = username.lower()
    is_white = (game.get("white", "") or "").lower() == u

    # Build eval map: move_number → cp (pawns, white-positive)
    eval_map: dict[int, float] = {}
    for e in evals:
        mn = e.get("move_number")
        cp = e.get("cp_score")
        mate = e.get("mate_in")
        if mn is None:
            continue
        if mate is not None:
            eval_map[mn] = 10.0 if (int(mate) > 0) else -10.0
        elif cp is not None:
            eval_map[mn] = max(-10.0, min(10.0, float(cp)))

    if not eval_map:
        return _empty_review()

    total_moves = max(eval_map.keys())

    # Material balance map: move_number → white_material - black_material
    material_map: dict[int, int] = {}
    for e in evals:
        mn = e.get("move_number")
        fen = e.get("fen")
        if mn is not None and fen:
            material_map[mn] = _material_balance(fen)

    # User drops
    user_drops = _compute_drops(eval_map, for_white=is_white)
    blunders     = [d for d in user_drops if d["drop"] >= BLUNDER_THRESH]
    mistakes     = [d for d in user_drops if MISTAKE_THRESH <= d["drop"] < BLUNDER_THRESH]
    inaccuracies = [d for d in user_drops if INACCURACY_THRESH <= d["drop"] < MISTAKE_THRESH]

    # Opponent drops
    opp_drops        = _compute_drops(eval_map, for_white=not is_white)
    opp_blunders     = [d for d in opp_drops if d["drop"] >= BLUNDER_THRESH]
    opp_mistakes     = [d for d in opp_drops if MISTAKE_THRESH <= d["drop"] < BLUNDER_THRESH]
    opp_inaccuracies = [d for d in opp_drops if INACCURACY_THRESH <= d["drop"] < MISTAKE_THRESH]

    biggest = max(user_drops, key=lambda d: d["drop"]) if user_drops else None
    biggest_drop_cp = int(round(biggest["drop"] * 100)) if biggest else 0
    critical_move = biggest["move"] if biggest else None
    critical_phase = biggest["phase"] if biggest else None

    text = _build_text(
        game, is_white, total_moves,
        blunders, mistakes, inaccuracies,
        opp_blunders, opp_mistakes, opp_inaccuracies,
        biggest, eval_map, opp_drops, material_map,
        evals,
    )

    return {
        "blunder_count": len(blunders),
        "mistake_count": len(mistakes),
        "inaccuracy_count": len(inaccuracies),
        "biggest_drop_cp": biggest_drop_cp,
        "critical_move_number": critical_move,
        "critical_phase": critical_phase,
        "text_summary": text,
    }


def _empty_review() -> dict:
    return {
        "blunder_count": 0,
        "mistake_count": 0,
        "inaccuracy_count": 0,
        "biggest_drop_cp": 0,
        "critical_move_number": None,
        "critical_phase": None,
        "text_summary": "No evaluation data available.",
    }


def _build_text(
    game: dict,
    is_white: bool,
    total_moves: int,
    blunders: list,
    mistakes: list,
    inaccuracies: list,
    opp_blunders: list,
    opp_mistakes: list,
    opp_inaccuracies: list,
    biggest: Optional[dict],
    eval_map: dict,
    opp_drops: list,
    material_map: dict,
    evals: list,
) -> str:
    parts = []

    result = game.get("result", "?")
    if is_white:
        outcome = "Win" if result == "1-0" else ("Loss" if result == "0-1" else "Draw")
    else:
        outcome = "Win" if result == "0-1" else ("Loss" if result == "1-0" else "Draw")

    opponent = game.get("black" if is_white else "white") or "?"
    full_moves = (total_moves + 1) // 2

    headline = _result_headline(game, is_white, outcome, evals)
    parts.append(f"{headline} · {full_moves} moves")

    # ── Game Story ────────────────────────────────────────────────────────────
    parts.extend(_game_story(
        game, is_white, total_moves,
        blunders, mistakes, opp_blunders, opp_mistakes,
        eval_map, material_map, evals,
    ))

    # ── Your errors ───────────────────────────────────────────────────────────
    parts.append("")
    parts.append("YOUR PLAY")
    if not blunders and not mistakes and not inaccuracies:
        parts.append("  Clean game — no significant errors detected.")
    else:
        err_parts = []
        if blunders:
            err_parts.append(f"{len(blunders)} blunder{'s' if len(blunders) != 1 else ''}")
        if mistakes:
            err_parts.append(f"{len(mistakes)} mistake{'s' if len(mistakes) != 1 else ''}")
        if inaccuracies:
            err_parts.append(
                f"{len(inaccuracies)} inaccurac{'ies' if len(inaccuracies) != 1 else 'y'}"
            )
        parts.append("  Errors: " + ", ".join(err_parts) + ".")

        # Biggest own error
        if biggest:
            before_user = biggest["before"] if is_white else -biggest["before"]
            after_user  = biggest["after"]  if is_white else -biggest["after"]
            drop_cp     = int(round(biggest["drop"] * 100))
            err_type    = (
                "Blunder" if biggest["drop"] >= BLUNDER_THRESH
                else "Mistake" if biggest["drop"] >= MISTAKE_THRESH
                else "Inaccuracy"
            )
            full_move_num = (biggest["move"] + 1) // 2
            parts.append(
                f"  Worst: {err_type} on move {full_move_num} ({biggest['phase']}) — "
                f"eval {_sign(before_user)} → {_sign(after_user)} ({drop_cp}cp lost)."
            )

        # Blunder phase breakdown
        by_phase: dict[str, int] = {"opening": 0, "middlegame": 0, "endgame": 0}
        for d in blunders:
            by_phase[d["phase"]] += 1
        phase_parts = [f"{v} in {k}" for k, v in by_phase.items() if v > 0]
        if phase_parts:
            parts.append("  Blunders by phase: " + ", ".join(phase_parts) + ".")

    # Opening assessment (eval after move ~20)
    opening_eval_mn = None
    for mn in range(20, 0, -1):
        if mn in eval_map:
            opening_eval_mn = mn
            break
    if opening_eval_mn:
        oe_raw = eval_map[opening_eval_mn]
        oe = oe_raw if is_white else -oe_raw
        if oe >= 1.5:
            desc = f"strong advantage ({_sign(oe)} pawns)"
        elif oe >= 0.3:
            desc = f"slight edge ({_sign(oe)} pawns)"
        elif oe >= -0.3:
            desc = f"balanced ({_sign(oe)} pawns)"
        elif oe >= -1.5:
            desc = f"slightly worse ({_sign(oe)} pawns)"
        else:
            desc = f"significantly worse ({_sign(oe)} pawns)"
        full_move_num = (opening_eval_mn + 1) // 2
        parts.append(f"  After opening (move {full_move_num}): {desc}.")

    # Peak advantage note
    if eval_map:
        peak = max(eval_map.values()) if is_white else max(-v for v in eval_map.values())
        if peak > 3.0 and outcome == "Loss":
            parts.append(
                f"  Note: Peak advantage was +{peak:.2f} pawns — a winning position was lost."
            )
        elif peak > 2.0 and outcome == "Draw":
            parts.append(
                f"  Note: Peak advantage was +{peak:.2f} pawns — a potential win ended in a draw."
            )

    # ── Material balance ──────────────────────────────────────────────────────
    if material_map:
        sorted_mns = sorted(material_map.keys())
        user_balances = [(mn, (material_map[mn] if is_white else -material_map[mn]))
                         for mn in sorted_mns]

        # Final material balance (from user's perspective)
        last_mn = sorted_mns[-1]
        final_balance = material_map[last_mn]
        user_final = final_balance if is_white else -final_balance

        # Peak / worst
        peak_mn, peak_adv  = max(user_balances, key=lambda x: x[1])
        worst_mn, worst_adv = min(user_balances, key=lambda x: x[1])

        # Duration stats
        stats = _material_duration_stats(user_balances)

        parts.append("")
        parts.append("MATERIAL")
        parts.append(f"  End of game: {_material_desc(user_final)}.")

        if peak_adv > 0 and peak_adv != user_final:
            full_mn = (peak_mn + 1) // 2
            parts.append(f"  Peak advantage: {_material_desc(peak_adv)} (move {full_mn}).")
        if worst_adv < 0 and worst_adv != user_final:
            full_mn = (worst_mn + 1) // 2
            parts.append(f"  Worst deficit: {_material_desc(worst_adv)} (move {full_mn}).")

        # Duration & average
        if stats:
            avg = stats["avg_balance"]
            ahead_pct  = stats["ahead_pct"]
            behind_pct = stats["behind_pct"]
            streak     = stats["max_streak"]

            avg_desc = (
                f"averaged {_sign(float(avg))} pts"
                if avg != 0
                else "averaged equal material"
            )
            parts.append(
                f"  Over the game you {avg_desc} — "
                f"ahead {ahead_pct}% of moves, behind {behind_pct}%."
            )
            if streak >= 10:
                streak_moves = streak // 2
                parts.append(
                    f"  Longest sustained material lead: ~{streak_moves} full moves ({streak} half-moves)."
                )

        # Annotate each blunder/mistake with immediate + sustained material impact
        for d in blunders + mistakes:
            mn = d["move"]
            prev_mn = mn - 1
            full_mn = (mn + 1) // 2
            err_type = "Blunder" if d["drop"] >= BLUNDER_THRESH else "Mistake"

            immediate_loss = None
            if prev_mn in material_map and mn in material_map:
                before_mat = material_map[prev_mn] if is_white else -material_map[prev_mn]
                after_mat  = material_map[mn]       if is_white else -material_map[mn]
                mat_lost = before_mat - after_mat
                if mat_lost >= 1:
                    immediate_loss = mat_lost

            sustained = _material_after_blunder(mn, material_map, is_white, sorted_mns, window=15)

            note_parts = []
            if immediate_loss is not None:
                note_parts.append(
                    f"lost {_material_desc(-immediate_loss).replace('down ', '')} of material immediately"
                )
            if sustained is not None:
                w          = sustained["window"]
                opp_count  = sustained["opp_count"]
                user_count = sustained["user_count"]

                def _edge_range(lo, hi):
                    fmt = lambda v: f"{v:.0f}" if v == int(v) else f"{v:.1f}"
                    return f"+{fmt(lo)}pt" if lo == hi else f"+{fmt(lo)}–{fmt(hi)}pt"

                if opp_count >= 8:
                    note_parts.append(
                        f"opponent held material edge for {opp_count} of next {w} half-moves "
                        f"({_edge_range(sustained['opp_min_edge'], sustained['opp_max_edge'])})"
                    )
                elif opp_count >= 4:
                    note_parts.append(
                        f"opponent gained temporary material edge ({opp_count}/{w} half-moves, "
                        f"{_edge_range(sustained['opp_min_edge'], sustained['opp_max_edge'])})"
                    )
                if user_count >= 8:
                    note_parts.append(
                        f"you held material edge for {user_count} of next {w} half-moves "
                        f"({_edge_range(sustained['user_min_edge'], sustained['user_max_edge'])})"
                    )
                elif user_count >= 4:
                    note_parts.append(
                        f"you maintained temporary material edge ({user_count}/{w} half-moves, "
                        f"{_edge_range(sustained['user_min_edge'], sustained['user_max_edge'])})"
                    )

            if note_parts:
                parts.append(
                    f"  Move {full_mn} ({err_type.lower()}): {'; '.join(note_parts)}."
                )

    # ── Opponent errors ───────────────────────────────────────────────────────
    parts.append("")
    parts.append(f"OPPONENT ({opponent})")
    if not opp_blunders and not opp_mistakes and not opp_inaccuracies:
        parts.append("  Clean game — no significant errors detected.")
    else:
        opp_err_parts = []
        if opp_blunders:
            opp_err_parts.append(f"{len(opp_blunders)} blunder{'s' if len(opp_blunders) != 1 else ''}")
        if opp_mistakes:
            opp_err_parts.append(f"{len(opp_mistakes)} mistake{'s' if len(opp_mistakes) != 1 else ''}")
        if opp_inaccuracies:
            opp_err_parts.append(
                f"{len(opp_inaccuracies)} inaccurac{'ies' if len(opp_inaccuracies) != 1 else 'y'}"
            )
        parts.append("  Errors: " + ", ".join(opp_err_parts) + ".")

        # Biggest opponent error
        opp_biggest = max(opp_drops, key=lambda d: d["drop"]) if opp_drops else None
        if opp_biggest:
            before_opp = opp_biggest["before"] if not is_white else -opp_biggest["before"]
            after_opp  = opp_biggest["after"]  if not is_white else -opp_biggest["after"]
            drop_cp    = int(round(opp_biggest["drop"] * 100))
            err_type   = (
                "Blunder" if opp_biggest["drop"] >= BLUNDER_THRESH
                else "Mistake" if opp_biggest["drop"] >= MISTAKE_THRESH
                else "Inaccuracy"
            )
            full_move_num = (opp_biggest["move"] + 1) // 2
            parts.append(
                f"  Worst: {err_type} on move {full_move_num} ({opp_biggest['phase']}) — "
                f"eval {_sign(before_opp)} → {_sign(after_opp)} ({drop_cp}cp lost)."
            )

        # Opponent blunder phase breakdown
        opp_by_phase: dict[str, int] = {"opening": 0, "middlegame": 0, "endgame": 0}
        for d in opp_blunders:
            opp_by_phase[d["phase"]] += 1
        opp_phase_parts = [f"{v} in {k}" for k, v in opp_by_phase.items() if v > 0]
        if opp_phase_parts:
            parts.append("  Blunders by phase: " + ", ".join(opp_phase_parts) + ".")

    return "\n".join(parts)
