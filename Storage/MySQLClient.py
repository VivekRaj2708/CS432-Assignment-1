from __future__ import annotations
import mysql.connector
from mysql.connector import Error
from Utils.Log import logger

from dotenv import load_dotenv
load_dotenv()
import os

p = os.getenv("p")


class MySQLClient:
    def __init__(
        self,
        host:     str = "localhost",
        port:     int = 3306,
        user:     str = "root",
        password: str = p,
        database: str = "ingestion",
    ):
        self.config = {
            "host":     host,
            "port":     port,
            "user":     user,
            "password": password,
            "database": database,
        }
        self._conn = None


    def connect(self) -> None:
        try:
            self._conn = mysql.connector.connect(**self.config)
            if self._conn.is_connected():
                logger.info("MySQLClient: connected to %s/%s",
                            self.config["host"], self.config["database"])
        except Error as exc:
            logger.error("MySQLClient: connection failed – %s", exc)
            raise

    def disconnect(self) -> None:
        if self._conn and self._conn.is_connected():
            self._conn.close()
            logger.info("MySQLClient: disconnected")

    def _ensure_connected(self) -> None:
        if not self._conn or not self._conn.is_connected():
            self.connect()

    def execute_log_file(self, filename: str = "sql_queries.log") -> dict:
        self._ensure_connected()
        cursor = self._conn.cursor()

        executed  = 0
        skipped   = 0
        errors    = 0

        try:
            with open(filename, "r") as f:
                raw = f.read()

            statements = [s.strip() for s in raw.split(";") if s.strip()]

            for stmt in statements:
                lines = [l.strip() for l in stmt.splitlines() if l.strip()]
                if not lines or all(l.startswith("--") for l in lines):
                    skipped += 1
                    continue

                try:
                    cursor.execute(stmt)
                    executed += 1
                except Error as exc:
                    if exc.errno in (1060, 1050):
                        logger.debug("MySQLClient: benign skip – %s", exc.msg)
                        skipped += 1
                    else:
                        logger.error("MySQLClient: error on statement:\n%s\n%s", stmt, exc)
                        errors += 1

            self._conn.commit()
            logger.info(
                "MySQLClient: executed=%d  skipped=%d  errors=%d",
                executed, skipped, errors,
            )

        except FileNotFoundError:
            logger.error("MySQLClient: log file not found – %s", filename)
            raise
        finally:
            cursor.close()

        return {"executed": executed, "skipped": skipped, "errors": errors}