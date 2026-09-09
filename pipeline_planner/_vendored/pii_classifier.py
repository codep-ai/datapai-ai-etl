"""
PII / sensitivity classification for source columns.

Ported from ag2_pipeline/compliance_tools.py (2026-09-09). Heuristics are
unchanged; the AG2 SwarmResult / DuckDB / pandas plumbing is removed so this
runs on plain Python lists.

Sensitivity levels:
  PII_HIGH    SSN, credit card, passport, credentials, health data
  FINANCIAL   salary, account numbers, routing numbers, IBAN
  PII_MEDIUM  email, phone, name, date of birth, gender
  PII_LOW     address, ZIP, IP address, geo
  PUBLIC      no sensitive content detected
"""
from __future__ import annotations

import re
from typing import Iterable, Tuple

SENSITIVITY_RANK: dict[str, int] = {
    "PII_HIGH": 4,
    "FINANCIAL": 3,
    "PII_MEDIUM": 2,
    "PII_LOW": 1,
    "PUBLIC": 0,
}

_VALUE_PATTERNS: dict[str, tuple[re.Pattern, str]] = {
    "email":       (re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"), "PII_MEDIUM"),
    "us_phone":    (re.compile(r"\b(\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}\b"), "PII_MEDIUM"),
    "ssn":         (re.compile(r"\b\d{3}[- ]\d{2}[- ]\d{4}\b"), "PII_HIGH"),
    "credit_card": (re.compile(r"\b(?:\d{4}[\s\-]?){3}\d{4}\b"), "PII_HIGH"),
    "iban":        (re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b"), "FINANCIAL"),
    "ip_address":  (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "PII_LOW"),
    "passport":    (re.compile(r"\b[A-Z]{1,2}\d{6,9}\b"), "PII_HIGH"),
}

_NAME_HEURISTICS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"\b(password|passwd|secret|token|api.?key|private.?key|auth.?key)\b", re.I), "credential", "PII_HIGH"),
    (re.compile(r"\b(ssn|social.?security|national.?id|sin|fiscal.?id|tax.?id|ein|itin)\b", re.I), "ssn", "PII_HIGH"),
    (re.compile(r"\b(passport|drivers?.?licen[sc]e|dl.?num|gov.?id)\b", re.I), "gov_id", "PII_HIGH"),
    (re.compile(r"\b(credit.?card|card.?num(?:ber)?|pan|cvv|cvc|card.?holder|expiry|exp.?date)\b", re.I), "credit_card", "PII_HIGH"),
    (re.compile(r"\b(account.?num(?:ber)?|acct.?num|routing|iban|swift|bic|bank.?account|sort.?code)\b", re.I), "bank_account", "FINANCIAL"),
    (re.compile(r"\b(salary|income|wage|compensation|bonus|revenue|profit|balance|net.?worth)\b", re.I), "financial_data", "FINANCIAL"),
    (re.compile(r"\b(medical|diagnosis|diagnos[ei]s|icd.?\d|health|prescription|patient|clinical|mrn|npi|nhs)\b", re.I), "health", "PII_HIGH"),
    (re.compile(r"\b(email|e.?mail|email.?address)\b", re.I), "email", "PII_MEDIUM"),
    (re.compile(r"\b(phone|mobile|cell|telephone|tel|fax)\b", re.I), "phone", "PII_MEDIUM"),
    (re.compile(r"\b(first.?name|last.?name|full.?name|given.?name|surname|maiden|middle.?name)\b", re.I), "name", "PII_MEDIUM"),
    (re.compile(r"\b(dob|birth.?date|date.?of.?birth|birthday|age|gender|sex|race|ethnicity|nationality)\b", re.I), "demographics", "PII_MEDIUM"),
    (re.compile(r"\b(street|address|addr|city|state|zip|postal|postcode|province)\b", re.I), "address", "PII_LOW"),
    (re.compile(r"\b(ip.?addr(?:ess)?|ipv4|ipv6|mac.?addr)\b", re.I), "ip_address", "PII_LOW"),
    (re.compile(r"\b(latitude|longitude|lat|lon|geo|location|coordinates)\b", re.I), "geo_data", "PII_LOW"),
]
# NOTE vs original: `country` and `region` were removed from the address heuristic.
# In market-reference data (exchange_ref.country, ticker_universe.region) they
# describe a venue, not a person, and flagged every reference table as PII_LOW.


_NUMERIC_TYPES = re.compile(
    r"^(numeric|decimal|double|real|float|int|bigint|smallint|integer|number|money|boolean|bool|date|time)", re.I
)


def classify_column(col: str, sample_values: Iterable, data_type: str = "") -> Tuple[str, str]:
    """
    Return (pii_type, sensitivity_level) for a column.
    Name heuristics first (fast), then regex scan of sample values (≥10% hit rate).

    `data_type` (optional): numeric/boolean/temporal columns skip the value scan —
    long decimals such as 317.1199951171875 otherwise match the phone/passport
    patterns (seen on datapai.prices.adj_close, 2026-09-09).
    """
    for pattern, pii_type, sensitivity in _NAME_HEURISTICS:
        if pattern.search(col):
            return pii_type, sensitivity

    if data_type and _NUMERIC_TYPES.match(data_type.strip()):
        return "none", "PUBLIC"

    str_values = [str(v) for v in sample_values if v is not None and str(v).strip()]
    if not str_values:
        return "none", "PUBLIC"

    for pii_type, (pattern, sensitivity) in _VALUE_PATTERNS.items():
        hit_count = sum(1 for v in str_values if pattern.search(v))
        if hit_count / len(str_values) >= 0.10:
            return pii_type, sensitivity

    return "none", "PUBLIC"


def max_sensitivity(levels: Iterable[str]) -> str:
    best = "PUBLIC"
    for lv in levels:
        if SENSITIVITY_RANK.get(lv, 0) > SENSITIVITY_RANK[best]:
            best = lv
    return best
