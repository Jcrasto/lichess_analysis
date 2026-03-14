"""Deterministic text summary generation for chess game reviews."""
import math
from typing import Optional

import chess
import chess.pgn

_STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
_CHECKPOINT_INTERVAL = 5  # full moves

BLUNDER_THRESH = 3.0    # pawns (cp_score is stored in pawns)
MISTAKE_THRESH = 1.5
INACCURACY_THRESH = 0.5

_PIECE_VALUES = {'P': 1, 'N': 3, 'B': 3, 'R': 5, 'Q': 9}


def _win_pct(cp_pawns: float) -> float:
    """Convert cp_score (in pawns, white-positive) to white win% (0–100)."""
    cp = cp_pawns * 100  # to centipawns
    return 50 + 50 * (2 / (1 + math.exp(-0.00368208 * cp)) - 1)


def _accuracy_pct(wp_before: float, wp_after: float) -> float:
    """Lichess accuracy% for one move given win% before/after (player's perspective)."""
    raw = 103.1668 * math.exp(-0.04354 * (wp_before - wp_after)) - 3.1669
    return max(0.0, min(100.0, raw))


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
    """Return all drops >= INACCURACY_THRESH for one side, with win%/accuracy data."""
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
        # Win% from the player's perspective (0–100)
        if for_white:
            wp_before = _win_pct(prev_cp)
            wp_after  = _win_pct(curr_cp)
        else:
            wp_before = 100.0 - _win_pct(prev_cp)
            wp_after  = 100.0 - _win_pct(curr_cp)
        wp_drop  = max(0.0, wp_before - wp_after)
        accuracy = _accuracy_pct(wp_before, wp_after)
        if drop >= INACCURACY_THRESH:
            drops.append({
                "move": mn,
                "drop": drop,
                "before": prev_cp,
                "after": curr_cp,
                "phase": _phase(mn),
                "wp_before": wp_before,
                "wp_after":  wp_after,
                "wp_drop":   wp_drop,
                "accuracy":  accuracy,
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


def _piece_summary(pieces: list) -> str:
    """Summarise a list of piece names with counts: ['pawn','pawn','knight'] → '2 pawns and a knight'."""
    if not pieces:
        return "nothing"
    order = ["queen", "rook", "bishop", "knight", "pawn"]
    counts: dict = {}
    for p in pieces:
        counts[p] = counts.get(p, 0) + 1
    parts = []
    for p in order:
        n = counts.get(p, 0)
        if n:
            parts.append(f"a {p}" if n == 1 else f"{n} {p}s")
    if len(parts) == 1:
        return parts[0]
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def _uci_to_san(uci: str, fen: str) -> str:
    """Convert a UCI move string to SAN given the board FEN. Falls back to UCI on error."""
    try:
        board = chess.Board(fen)
        move  = chess.Move.from_uci(uci)
        return board.san(move)
    except Exception:
        return uci


def _game_story(
    game: dict,
    is_white: bool,
    total_moves: int,
    blunders: list,
    mistakes: list,
    inaccuracies: list,
    opp_blunders: list,
    opp_mistakes: list,
    opp_inaccuracies: list,
    eval_map: dict,
    material_map: dict,
    evals: list,
) -> list:
    """Return the GAME STORY section as a complete chronological log."""
    fen_map = {
        e["move_number"]: e["fen"]
        for e in evals
        if e.get("move_number") is not None and e.get("fen")
    }
    # best_move at ply N = best move from the position AFTER ply N.
    # For an error at ply N, the best alternative = best_move_map[N-1].
    best_move_map = {
        e["move_number"]: e.get("best_move")
        for e in evals
        if e.get("move_number") is not None
    }

    opponent  = game.get("black" if is_white else "white") or "opponent"
    username  = game.get("white" if is_white else "black") or "you"
    result    = game.get("result", "?")
    if is_white:
        outcome = "Win" if result == "1-0" else ("Loss" if result == "0-1" else "Draw")
    else:
        outcome = "Win" if result == "0-1" else ("Loss" if result == "1-0" else "Draw")

    full_moves = (total_moves + 1) // 2

    parts = ["", "GAME STORY"]

    # ── Build event lookup structures ─────────────────────────────────────────

    # error_by_ply: ply → [(is_user, drop_dict)]
    error_by_ply: dict = {}
    for d in blunders + mistakes + inaccuracies:
        error_by_ply.setdefault(d["move"], []).append((True, d))
    for d in opp_blunders + opp_mistakes + opp_inaccuracies:
        error_by_ply.setdefault(d["move"], []).append((False, d))

    # mat_change_plies: plies where material changed
    sorted_mat_plies = sorted(material_map.keys())
    mat_change_plies: set = set()
    prev_mat_ply = None
    for ply in sorted_mat_plies:
        if prev_mat_ply is not None and material_map[ply] != material_map[prev_mat_ply]:
            mat_change_plies.add(ply)
        prev_mat_ply = ply

    # checkpoint_plies: every _CHECKPOINT_INTERVAL full moves, keyed as ply + 0.5
    # so they sort AFTER the last event of that full move
    checkpoint_plies = []
    for fm in range(_CHECKPOINT_INTERVAL, full_moves + _CHECKPOINT_INTERVAL, _CHECKPOINT_INTERVAL):
        cp_fm = min(fm, full_moves)
        checkpoint_plies.append(cp_fm * 2 + 0.5)

    # Unified sorted event list: (sort_key, kind, data)
    # kind: "error_or_mat" (int ply) or "checkpoint" (float ply)
    all_events: list = []
    for ply in sorted(set(error_by_ply.keys()) | mat_change_plies):
        all_events.append((ply, "move", ply))
    for cp_key in checkpoint_plies:
        fm = int((cp_key - 0.5) // 2)
        all_events.append((cp_key, "checkpoint", fm))
    all_events.sort(key=lambda x: x[0])

    # For checkpoints: track which FEN we last snapshotted at
    prev_checkpoint_fen = _STARTING_FEN
    prev_checkpoint_fm  = 0

    # ── Process events ────────────────────────────────────────────────────────
    for sort_key, kind, data in all_events:

        if kind == "checkpoint":
            fm = data
            if fm > full_moves:
                fm = full_moves
            # Find the closest fen at or before ply fm*2
            target_ply = fm * 2
            available_fens = [p for p in sorted(fen_map.keys()) if p <= target_ply]
            if not available_fens:
                continue
            curr_fen = fen_map[available_fens[-1]]

            # Cumulative pieces lost since last checkpoint
            wl, bl = _pieces_lost(prev_checkpoint_fen, curr_fen)
            user_lost_cp = wl if is_white else bl
            opp_lost_cp  = bl if is_white else wl

            # Current material advantage
            closest_ply = available_fens[-1]
            user_mat = (material_map[closest_ply] if is_white else -material_map[closest_ply]) \
                       if closest_ply in material_map else 0

            # Format losses
            user_lost_str = _piece_summary(user_lost_cp)
            opp_lost_str  = _piece_summary(opp_lost_cp)

            since_label = (
                f"moves {prev_checkpoint_fm + 1}–{fm}"
                if prev_checkpoint_fm > 0
                else f"moves 1–{fm}"
            )

            if user_mat > 0:
                adv_str = f"Material advantage: {username} +{user_mat} ({_material_desc(user_mat)})."
            elif user_mat < 0:
                adv_str = f"Material advantage: {opponent} +{abs(user_mat)} ({_material_desc(-user_mat)})."
            else:
                adv_str = "Material is equal."

            # Win% at this checkpoint
            eval_str = ""
            if closest_ply in eval_map:
                raw_eval = eval_map[closest_ply]
                user_wp = _win_pct(raw_eval) if is_white else 100.0 - _win_pct(raw_eval)
                if user_wp >= 65:
                    eval_str = f" Win%: {username} {user_wp:.1f}%."
                elif user_wp >= 55:
                    eval_str = f" Win%: slight edge for {username} ({user_wp:.1f}%)."
                elif user_wp >= 45:
                    eval_str = f" Win%: balanced ({user_wp:.1f}%)."
                elif user_wp >= 35:
                    eval_str = f" Win%: slight edge for {opponent} ({user_wp:.1f}%)."
                else:
                    eval_str = f" Win%: {opponent} favoured ({user_wp:.1f}%)."

            parts.append(
                f"  ── After move {fm} ({since_label}) ──"
                f" {username} lost: {user_lost_str}."
                f" {opponent} lost: {opp_lost_str}."
                f" {adv_str}{eval_str}"
            )

            prev_checkpoint_fen = curr_fen
            prev_checkpoint_fm  = fm
            continue

        # kind == "move"
        ply     = data
        full_mn = (ply + 1) // 2
        phase   = _phase(ply)

        # Current material string — name the player so it's unambiguous
        mat_str = ""
        if ply in material_map:
            user_mat = material_map[ply] if is_white else -material_map[ply]
            pts = abs(user_mat)
            pts_str = f"{pts}pt{'s' if pts != 1 else ''}"
            if user_mat > 0:
                mat_str = f" {username} up {pts_str}."
            elif user_mat < 0:
                mat_str = f" {opponent} up {pts_str}."
            else:
                mat_str = " Material equal."

        if ply in error_by_ply:
            for is_user_event, d in sorted(error_by_ply[ply], key=lambda x: not x[0]):
                err_label = (
                    "Blunder"    if d["drop"] >= BLUNDER_THRESH
                    else "Mistake"    if d["drop"] >= MISTAKE_THRESH
                    else "Inaccuracy"
                )
                eval_str = (
                    f"win% {d['wp_before']:.1f}→{d['wp_after']:.1f} "
                    f"(-{d['wp_drop']:.1f}%) | acc {d['accuracy']:.1f}%"
                )

                # Best move: what should have been played (from the position before this ply)
                best_uci = best_move_map.get(ply - 1)
                prev_fen_for_best = fen_map.get(ply - 1)
                if best_uci and prev_fen_for_best:
                    best_str = f" Best: {_uci_to_san(best_uci, prev_fen_for_best)}."
                else:
                    best_str = ""

                user_lost: list = []
                opp_lost:  list = []
                for check_ply in (ply, ply + 1):
                    fa = fen_map.get(check_ply - 1)
                    fb = fen_map.get(check_ply)
                    if fa and fb:
                        wl, bl = _pieces_lost(fa, fb)
                        user_lost.extend(wl if is_white else bl)
                        opp_lost.extend(bl if is_white else wl)

                if is_user_event:
                    actor = f"Your {err_label.lower()}"
                    if user_lost and opp_lost:
                        cap = (f" You traded your {_piece_list(user_lost)}"
                               f" for {opponent}'s {_piece_list(opp_lost)}.")
                    elif user_lost:
                        cap = f" Your {_piece_list(user_lost)} was lost."
                    elif opp_lost:
                        cap = f" You captured {opponent}'s {_piece_list(opp_lost)}."
                    else:
                        cap = " No piece immediately lost — positional error."
                else:
                    actor = f"{opponent}'s {err_label.lower()}"
                    if opp_lost and user_lost:
                        cap = (f" {opponent} traded their {_piece_list(opp_lost)}"
                               f" for your {_piece_list(user_lost)}.")
                    elif opp_lost:
                        cap = f" {opponent}'s {_piece_list(opp_lost)} was lost."
                    elif user_lost:
                        cap = f" Your {_piece_list(user_lost)} was captured."
                    else:
                        cap = " No piece immediately lost — positional error."

                parts.append(
                    f"  Move {full_mn} ({phase}) — {actor}: {eval_str}.{cap}{best_str}{mat_str}"
                )

        elif ply in mat_change_plies:
            fa = fen_map.get(ply - 1)
            fb = fen_map.get(ply)
            if not (fa and fb):
                continue

            wl, bl     = _pieces_lost(fa, fb)
            user_lost  = wl if is_white else bl
            opp_lost   = bl if is_white else wl
            user_moved = ((ply % 2 == 1) == is_white)

            if user_lost and opp_lost:
                if user_moved:
                    desc = (f"You traded your {_piece_list(user_lost)}"
                            f" for {opponent}'s {_piece_list(opp_lost)}.")
                else:
                    desc = (f"{opponent} traded their {_piece_list(opp_lost)}"
                            f" for your {_piece_list(user_lost)}.")
            elif opp_lost:
                desc = (f"You captured {opponent}'s {_piece_list(opp_lost)}."
                        if user_moved
                        else f"{opponent}'s {_piece_list(opp_lost)} was taken.")
            elif user_lost:
                desc = (f"Your {_piece_list(user_lost)} was taken."
                        if user_moved
                        else f"{opponent} captured your {_piece_list(user_lost)}.")
            else:
                continue

            parts.append(f"  Move {full_mn} ({phase}) — {desc}{mat_str}")

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

    biggest = max(user_drops, key=lambda d: d["wp_drop"]) if user_drops else None
    biggest_drop_cp = int(round(biggest["drop"] * 100)) if biggest else 0
    biggest_win_pct_drop = round(biggest["wp_drop"], 2) if biggest else 0.0
    critical_move = biggest["move"] if biggest else None
    critical_phase = biggest["phase"] if biggest else None

    # Average Lichess accuracy across all user moves
    all_accuracies = []
    for mn in sorted(eval_map.keys()):
        if mn == 0:
            continue
        prev_mn = mn - 1
        if prev_mn not in eval_map:
            continue
        if (mn % 2 == 1) != is_white:
            continue
        prev_cp = eval_map[prev_mn]
        curr_cp = eval_map[mn]
        if is_white:
            wp_b = _win_pct(prev_cp)
            wp_a = _win_pct(curr_cp)
        else:
            wp_b = 100.0 - _win_pct(prev_cp)
            wp_a = 100.0 - _win_pct(curr_cp)
        all_accuracies.append(_accuracy_pct(wp_b, wp_a))
    lichess_accuracy_percentage = round(sum(all_accuracies) / len(all_accuracies), 1) if all_accuracies else 0.0

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
        "biggest_win_pct_drop": biggest_win_pct_drop,
        "lichess_accuracy_percentage": lichess_accuracy_percentage,
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
        "biggest_win_pct_drop": 0.0,
        "lichess_accuracy_percentage": 0.0,
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
    result = game.get("result", "?")
    if is_white:
        outcome = "Win" if result == "1-0" else ("Loss" if result == "0-1" else "Draw")
    else:
        outcome = "Win" if result == "0-1" else ("Loss" if result == "1-0" else "Draw")

    full_moves = (total_moves + 1) // 2

    white_player = game.get("white") or "?"
    black_player = game.get("black") or "?"
    white_elo    = game.get("white_elo")
    black_elo    = game.get("black_elo")
    white_str    = f"{white_player}{f' ({white_elo})' if white_elo else ''}"
    black_str    = f"{black_player}{f' ({black_elo})' if black_elo else ''}"

    headline = _result_headline(game, is_white, outcome, evals)

    parts = [
        f"{headline} · {full_moves} moves",
        f"White: {white_str}  |  Black: {black_str}",
    ]

    parts.extend(_game_story(
        game, is_white, total_moves,
        blunders, mistakes, inaccuracies,
        opp_blunders, opp_mistakes, opp_inaccuracies,
        eval_map, material_map, evals,
    ))

    return "\n".join(parts)
