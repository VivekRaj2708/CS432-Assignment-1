# Storage/MongoClient.py

from __future__ import annotations
import re
import json
from pymongo import MongoClient as PyMongoClient
from pymongo.errors import PyMongoError
from Utils.Log import logger


INSERT_RE = re.compile(r"^db\.(\w+)\.insertOne\((\{.*\})\);?$")


def _bare_keys_to_json(s: str) -> str:
    """
    Convert a MongoDB JS object literal with bare keys to valid JSON.

    Handles:
      - bare keys:          {name: "x"}              -> {"name": "x"}
      - nested objects:     {a: {b: 1}}               -> {"a": {"b": 1}}
      - arrays of scalars:  {tags: [1, 2, 3]}         -> {"tags": [1, 2, 3]}
      - arrays of strings:  {tags: ["a", "b"]}        -> {"tags": ["a", "b"]}
      - already-quoted:     {"a": 1}                  -> {"a": 1}  (untouched)
      - null/true/false:    {x: null, y: true}        -> {"x": null, "y": true}
      - numeric/float/neg:  {gps: -11.88, alt: 58.6}  -> as-is

    Key fix vs naive regex: array_depth tracking means commas inside
    [1, 2, 3] never trigger bare-key quoting.
    """
    result      = []
    i           = 0
    n           = len(s)
    in_str      = False
    array_depth = 0

    while i < n:
        c = s[i]

        # Track string boundaries
        if c == '"' and (i == 0 or s[i-1] != '\\'):
            in_str = not in_str
            result.append(c)
            i += 1
            continue

        if in_str:
            result.append(c)
            i += 1
            continue

        # Track array depth — commas inside arrays must not trigger key quoting
        if c == '[':
            array_depth += 1
            result.append(c)
            i += 1
            continue

        if c == ']':
            array_depth -= 1
            result.append(c)
            i += 1
            continue

        # Bare key detection: only at object level (not inside an array)
        if c in ('{', ',') and array_depth == 0:
            result.append(c)
            i += 1
            # Preserve whitespace
            while i < n and s[i] in ' \t\n\r':
                result.append(s[i])
                i += 1
            # If next char is not already a quote, closing brace, or bracket
            # then it's a bare key — wrap it
            if i < n and s[i] not in ('"', '}', ']'):
                j = i
                while j < n and s[j] not in (':', ' ', '\t', '\n', '\r'):
                    j += 1
                bare = s[i:j]
                if bare:
                    result.append('"')
                    result.extend(bare)
                    result.append('"')
                    i = j
            continue

        result.append(c)
        i += 1

    return ''.join(result)


def _parse_line(line: str):
    """
    Parse one log line into (collection_name, document_dict).
    Returns None for blank lines or lines that fail to parse.
    """
    line = line.strip()
    if not line:
        return None

    match = INSERT_RE.match(line)
    if not match:
        logger.warning("MongoClient: unrecognised line: %s", line[:80])
        return None

    collection = match.group(1)
    doc_str    = match.group(2)
    json_str   = _bare_keys_to_json(doc_str)

    try:
        doc = json.loads(json_str)
    except json.JSONDecodeError as exc:
        logger.error(
            "MongoClient: JSON parse failed – %s\n"
            "  original : %s\n"
            "  converted: %s",
            exc, doc_str[:120], json_str[:120],
        )
        return None

    return collection, doc


class MongoDBClient:
    def __init__(
        self,
        uri:     str = "mongodb://localhost:27017",
        db_name: str = "ingestion",
    ):
        self.uri     = uri
        self.db_name = db_name
        self._client = None
        self._db     = None

    def connect(self) -> None:
        try:
            self._client = PyMongoClient(self.uri)
            self._db     = self._client[self.db_name]
            self._client.admin.command("ping")
            logger.info("MongoDBClient: connected to %s / %s", self.uri, self.db_name)
        except PyMongoError as exc:
            logger.error("MongoDBClient: connection failed – %s", exc)
            raise

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            logger.info("MongoDBClient: disconnected")

    def _ensure_connected(self) -> None:
        if self._client is None:
            self.connect()

    def execute_log_file(self, filename: str = "mongo_queries.log") -> dict:
        """
        Read the log file and execute every insertOne line against MongoDB.
        Returns a summary dict: {inserted, skipped, errors}
        """
        self._ensure_connected()

        inserted = 0
        skipped  = 0
        errors   = 0

        try:
            with open(filename, "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            logger.error("MongoDBClient: log file not found – %s", filename)
            raise

        for lineno, line in enumerate(lines, start=1):
            parsed = _parse_line(line)

            if parsed is None:
                skipped += 1
                continue

            collection, doc = parsed

            try:
                self._db[collection].insert_one(doc)
                inserted += 1
            except PyMongoError as exc:
                logger.error(
                    "MongoDBClient: insert failed line %d – %s", lineno, exc
                )
                errors += 1

        logger.info(
            "MongoDBClient: %s — inserted=%d  skipped=%d  errors=%d",
            filename, inserted, skipped, errors,
        )
        return {"inserted": inserted, "skipped": skipped, "errors": errors}