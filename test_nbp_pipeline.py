import sqlite3
from datetime import date, timedelta

import pytest

import nbp_pipeline as p


def raw(code, d, mid):
    return {"code": code, "effectiveDate": d, "mid": mid}


# ---------- TRANSFORM ----------
def test_transform_valid_record_scaled_to_int():
    assert p.transform([raw("usd", "2026-01-05", 3.6789)]) == [("USD", "2026-01-05", 36789)]


@pytest.mark.parametrize("bad", [
    raw("USD", None, 3.6),
    raw("USD", "2026-01-05", None),
    raw("USD", "2026-01-05", 0),
    raw("USD", "2026-01-05", -1.2),
    raw("USD", "nie-data", 3.6),
    raw("USD", "2026-01-05", "abc"),
    {"code": "USD"},
])
def test_transform_rejects_bad_records(bad):
    assert p.transform([bad]) == []


def test_transform_dedupes_per_currency():
    rows = [raw("USD", "2026-01-05", 3.6), raw("USD", "2026-01-05", 3.7), raw("EUR", "2026-01-05", 4.2)]
    out = p.transform(rows)
    assert len(out) == 2
    assert ("USD", "2026-01-05", 36000) in out


# ---------- LOAD ----------
@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    p.init_db(c)
    yield c
    c.close()


def test_load_is_idempotent_and_upserts(conn):
    p.load(conn, [("USD", "2026-01-05", 36000)])
    p.load(conn, [("USD", "2026-01-05", 36100), ("USD", "2026-01-06", 36200)])
    rows = conn.execute("SELECT * FROM currency_rates ORDER BY rate_date").fetchall()
    assert rows == [("USD", "2026-01-05", 36100), ("USD", "2026-01-06", 36200)]


def test_last_loaded_date(conn):
    assert p.last_loaded_date(conn, "USD") is None
    p.load(conn, [("USD", "2026-01-05", 36000), ("USD", "2026-01-09", 36000)])
    assert p.last_loaded_date(conn, "usd") == date(2026, 1, 9)


# ---------- ANALYTICS ----------
def seed(conn, currency, n, base=40000):
    start = date(2026, 1, 1)
    p.load(conn, [(currency, (start + timedelta(days=i)).isoformat(), base + i * 100) for i in range(n)])


def test_ma7_is_null_until_window_full(conn):
    seed(conn, "USD", 10)
    rows = sorted(p.analyze(conn, limit=100), key=lambda r: r[1])
    assert all(r[5] is None for r in rows[:6])       # pierwsze 6 dni: brak pełnego okna
    assert rows[6][5] is not None
    assert rows[6][5] == pytest.approx(4.03)         # średnia z 4.00..4.06


def test_currencies_are_partitioned(conn):
    seed(conn, "USD", 8, base=40000)
    seed(conn, "EUR", 8, base=43000)
    rows = p.analyze(conn, limit=100)
    first = {r[0]: r for r in rows if r[1] == "2026-01-01"}
    assert first["USD"][3] is None and first["EUR"][3] is None   # LAG nie „przecieka” między walutami


def test_limit_applies_per_currency(conn):
    seed(conn, "USD", 20)
    seed(conn, "EUR", 20)
    rows = p.analyze(conn, limit=3)
    assert len(rows) == 6


def test_fmt_handles_none():
    assert p.fmt(None) == "N/A"
    assert p.fmt(1.5, ".2f", True, "%") == "+1.50%"


def test_date_chunks_cover_range_without_gaps():
    s, e = date(2024, 1, 1), date(2026, 1, 1)
    chunks = list(p.date_chunks(s, e))
    assert chunks[0][0] == s and chunks[-1][1] == e
    for (_, end1), (start2, _) in zip(chunks, chunks[1:]):
        assert start2 == end1 + timedelta(days=1)
    assert all((b - a).days + 1 <= p.MAX_RANGE_DAYS for a, b in chunks)
