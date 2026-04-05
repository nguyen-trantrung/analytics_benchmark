import mysql.connector
import os
import time
import sys
import argparse
from typing import Dict, List, Tuple
from contextlib import contextmanager
import logging
import difflib
from prettytable import PrettyTable

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


class DatabaseChoiceAction(argparse.Action):
    """Custom action that provides suggestions for invalid database choices"""

    def __call__(self, parser, namespace, values, option_string=None):
        valid_choices = ["doris", "starrocks", "clickhouse", "tidb", "columnstore"]

        if values not in valid_choices:
            # Find closest matches
            close_matches = difflib.get_close_matches(
                values, valid_choices, n=3, cutoff=0.6
            )
            error_msg = (
                f"invalid choice: '{values}' (choose from {', '.join(valid_choices)})"
            )

            if close_matches:
                error_msg += f"\n\nDid you mean: {', '.join(close_matches)}?"

            parser.error(f"argument --database: {error_msg}")

        setattr(namespace, self.dest, values)


DATABASE_CONNECTIONS = {
    "tidb": {
        "host": "127.0.0.1",
        "port": 4000,
        "user": "root",
        "password": "",
        "charset": "utf8mb4",
        "collation": "utf8mb4_general_ci",
        "database": "bts",
    },
}

MAX_RETRIES = 3
RETRY_DELAY = 5


class DatabaseBenchmark:
    def __init__(
        self, database_type: str, config_overrides: Dict = None, engine: str = "both"
    ):
        """Initialize database benchmark.

        Args:
            database_type: Type of database (doris, starrocks, clickhouse, tidb, columnstore)
            config_overrides: Optional dictionary to override connection parameters
            engine: Storage engine for TiDB (tikv, tiflash, both, compare)
        """
        self.database_type = database_type
        self.db_config = DATABASE_CONNECTIONS[database_type].copy()

        if config_overrides:
            self.db_config.update(config_overrides)

        self.engine = engine
        self.conn = None
        self.cursor = None

    @contextmanager
    def connection(self):
        """Context manager for database connections"""
        try:
            self.conn = self._connect_with_fallback()
            self.cursor = self.conn.cursor(buffered=True)
            yield self.conn, self.cursor
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

    def _connect_with_fallback(self) -> mysql.connector.MySQLConnection:
        """Attempt connection with fallback to 'default' user if needed"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            logger.info(f"Connected successfully with user: {self.db_config['user']}")
            return conn
        except mysql.connector.Error as err:
            if self._is_auth_error(err) and self.db_config["user"] != "default":
                logger.info(
                    f"Authentication failed with '{self.db_config['user']}', trying 'default'..."
                )
                self.db_config["user"] = "default"
                try:
                    conn = mysql.connector.connect(**self.db_config)
                    logger.info(
                        f"Connected successfully with user: {self.db_config['user']}"
                    )
                    return conn
                except mysql.connector.Error as fallback_err:
                    logger.error(f"Failed to connect with both users: {fallback_err}")
                    sys.exit(1)
            else:
                logger.error(f"Database connection error: {err}")
                sys.exit(1)

    @staticmethod
    def _is_auth_error(err: mysql.connector.Error) -> bool:
        """Check if error is authentication related"""
        error_str = str(err).lower()
        auth_patterns = [
            "access denied",
            "authentication",
            "unknown user",
            "user",
            "password",
            "denied",
        ]
        return any(pattern in error_str for pattern in auth_patterns)

    @staticmethod
    def _is_memory_error(err: mysql.connector.Error) -> bool:
        """Check if error is memory related"""
        error_str = str(err).lower()
        return "mem_limit_exceeded" in error_str or "memory not enough" in error_str

    @staticmethod
    def _is_timeout_error(err: mysql.connector.Error) -> bool:
        """Check if error is timeout related"""
        error_str = str(err).lower()
        return "timeout" in error_str or "cancelled" in error_str

    def _set_engine_isolation(self, cursor, engine: str = None) -> None:
        """Set TiDB isolation read engines based on engine parameter"""
        if self.database_type != "tidb":
            return

        engine_to_use = engine if engine is not None else self.engine

        if engine_to_use == "tikv":
            cursor.execute("SET SESSION tidb_isolation_read_engines = 'tikv'")
            logger.info("[CONFIG] Set TiDB isolation read engines to TiKV")
        elif engine_to_use == "tiflash":
            cursor.execute("SET SESSION tidb_isolation_read_engines = 'tiflash'")
            logger.info("[CONFIG] Set TiDB isolation read engines to TiFlash")
        # For 'both' and 'compare', let CBO choose (no session variable set)

    def _execute_statements(self, cursor, sql_script: str) -> None:
        """Execute SQL statements from script"""
        statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]

        for statement in statements:
            cursor.execute(statement)

            if cursor.with_rows:
                cursor.fetchall()

            while cursor.nextset():
                pass

            self.conn.commit()

    def execute_query_with_retry(
        self, sql_script: str, filename: str
    ) -> Tuple[bool, float]:
        """Execute query with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                start_time = time.time()

                if not self.conn or not self.conn.is_connected():
                    self.conn = self._connect_with_fallback()
                    self.cursor = self.conn.cursor(buffered=True)

                self._execute_statements(self.cursor, sql_script)

                elapsed_time = time.time() - start_time
                minutes, seconds = divmod(elapsed_time, 60)
                logger.info(
                    f"[OK] Executed {filename} successfully in {int(minutes)}m {seconds:.2f}s (attempt {attempt + 1})"
                )
                return True, elapsed_time

            except mysql.connector.Error as err:
                if self._is_memory_error(err) or self._is_timeout_error(err):
                    if attempt < MAX_RETRIES - 1:
                        error_type = (
                            "Memory" if self._is_memory_error(err) else "Timeout"
                        )
                        logger.warning(
                            f"[WARN] {error_type} error on attempt {attempt + 1} for {filename}. Retrying in {RETRY_DELAY} seconds..."
                        )
                        time.sleep(RETRY_DELAY)

                        # Reconnect for memory errors
                        if self._is_memory_error(err):
                            if self.cursor:
                                self.cursor.close()
                            if self.conn:
                                self.conn.close()
                            time.sleep(2)
                        continue
                    else:
                        error_type = (
                            "memory constraints"
                            if self._is_memory_error(err)
                            else "timeout"
                        )
                        logger.error(
                            f"[FAIL] Failed to execute {filename} after {MAX_RETRIES} attempts due to {error_type}: {err}"
                        )
                        return False, 0

                elif self._is_auth_error(err) and self.db_config["user"] == "root":
                    logger.warning(
                        f"[WARN] Privilege error detected. This will be handled by the main loop."
                    )
                    raise

                else:
                    logger.error(f"[FAIL] Error executing {filename}: {err}")
                    return False, 0

        return False, 0

    def get_sql_files(self, queries_folder: str) -> List[str]:
        """Get sorted list of SQL files"""
        if not os.path.exists(queries_folder):
            logger.error(f"Queries folder not found: {queries_folder}")
            sys.exit(1)

        sql_files = [f for f in os.listdir(queries_folder) if f.endswith(".sql")]

        if not sql_files:
            logger.error(f"No SQL files found in {queries_folder}")
            sys.exit(1)

        # Sort by numeric prefix
        try:
            return sorted(sql_files, key=lambda x: int(x.split(".")[0]))
        except ValueError:
            # Fallback to alphabetical sort if numeric prefix fails
            return sorted(sql_files)

    def run_benchmarks(self, queries_folder: str = "queries/sql") -> None:
        """Run all benchmarks.

        If engine is 'compare', runs benchmarks with TiKV and TiFlash isolation
        and prints comparison tables. Otherwise, uses specified engine isolation
        for TiDB (tikv, tiflash, or both for CBO).
        """
        if self.engine == "compare":
            self._run_comparison_mode(queries_folder)
            return

        sql_files = self.get_sql_files(queries_folder)
        logger.info(
            f"[START] Running {len(sql_files)} queries on {self.database_type.upper()}"
        )

        successful_queries = []
        failed_queries = []
        total_time = 0

        with self.connection():
            for sql_file in sql_files:
                logger.info(f"\n[PROC] Processing {sql_file}...")
                file_path = os.path.join(queries_folder, sql_file)

                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        sql_script = file.read()

                    if not sql_script.strip():
                        logger.warning(f"[WARN] Empty SQL file: {sql_file}")
                        continue

                    success, query_time = self.execute_query_with_retry(
                        sql_script, sql_file
                    )

                    if success:
                        successful_queries.append((sql_file, query_time))
                        total_time += query_time
                    else:
                        failed_queries.append(sql_file)

                except FileNotFoundError:
                    logger.error(f"[FAIL] File not found: {sql_file}")
                    failed_queries.append(sql_file)
                except UnicodeDecodeError:
                    logger.error(
                        f"[FAIL] Unable to read file (encoding issue): {sql_file}"
                    )
                    failed_queries.append(sql_file)
                except mysql.connector.Error as err:
                    if self._is_auth_error(err) and self.db_config["user"] == "root":
                        logger.info(
                            f"[PROC] Privilege error detected. Switching to 'default' user and restarting..."
                        )
                        self._restart_with_default_user(sql_files, queries_folder)
                        return
                    else:
                        logger.error(f"[FAIL] Error with {sql_file}: {err}")
                        failed_queries.append(sql_file)

        self._print_summary(successful_queries, failed_queries, total_time)

        if failed_queries:
            sys.exit(1)

    def _run_comparison_mode(self, queries_folder: str) -> None:
        """Run benchmarks with both TiKV and TiFlash isolation and compare results."""
        sql_files = self.get_sql_files(queries_folder)

        try:
            # Run with TiKV isolation
            logger.info(
                f"[START] Running {len(sql_files)} queries on {self.database_type.upper()} (TiKV mode)..."
            )
            tikv_results = self._run_with_engine_isolation(queries_folder, "tikv")

            # Run with TiFlash isolation
            logger.info(
                f"[START] Running {len(sql_files)} queries on {self.database_type.upper()} (TiFlash mode)..."
            )
            tiflash_results = self._run_with_engine_isolation(queries_folder, "tiflash")

            # Print comparison tables
            self._print_comparison_tables(tikv_results, tiflash_results)

            # Exit with failure if any queries failed in either run
            if tikv_results[1] or tiflash_results[1]:
                sys.exit(1)

        except mysql.connector.Error as err:
            if self._is_auth_error(err) and self.db_config["user"] == "root":
                logger.info(
                    f"[PROC] Privilege error detected. Switching to 'default' user and restarting..."
                )
                self._restart_with_default_user(sql_files, queries_folder)
                return
            else:
                raise

    def _run_with_engine_isolation(
        self, queries_folder: str, engine: str
    ) -> Tuple[List[Tuple[str, float]], List[str], float]:
        """Run benchmarks with specific engine isolation and return results."""
        sql_files = self.get_sql_files(queries_folder)
        successful_queries = []
        failed_queries = []
        total_time = 0

        with self.connection():
            # Set engine isolation for TiDB
            if self.database_type == "tidb" and engine in ("tikv", "tiflash"):
                self._set_engine_isolation(self.cursor, engine)

            for sql_file in sql_files:
                logger.info(f"\n[PROC] Processing {sql_file}...")
                file_path = os.path.join(queries_folder, sql_file)

                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        sql_script = file.read()

                    if not sql_script.strip():
                        logger.warning(f"[WARN] Empty SQL file: {sql_file}")
                        continue

                    success, query_time = self.execute_query_with_retry(
                        sql_script, sql_file
                    )

                    if success:
                        successful_queries.append((sql_file, query_time))
                        total_time += query_time
                    else:
                        failed_queries.append(sql_file)

                except FileNotFoundError:
                    logger.error(f"[FAIL] File not found: {sql_file}")
                    failed_queries.append(sql_file)
                except UnicodeDecodeError:
                    logger.error(
                        f"[FAIL] Unable to read file (encoding issue): {sql_file}"
                    )
                    failed_queries.append(sql_file)
                except mysql.connector.Error as err:
                    if self._is_auth_error(err) and self.db_config["user"] == "root":
                        logger.info(
                            f"[PROC] Privilege error detected. Switching to 'default' user and restarting..."
                        )
                        # Cannot handle here, need to restart entire benchmark
                        raise
                    else:
                        logger.error(f"[FAIL] Error with {sql_file}: {err}")
                        failed_queries.append(sql_file)

        return successful_queries, failed_queries, total_time

    def _print_comparison_tables(
        self,
        tikv_results: Tuple[List[Tuple[str, float]], List[str], float],
        tiflash_results: Tuple[List[Tuple[str, float]], List[str], float],
    ) -> None:
        """Print ASCII tables comparing TiKV and TiFlash performance."""
        tikv_successful, tikv_failed, tikv_total = tikv_results
        tiflash_successful, tiflash_failed, tiflash_total = tiflash_results

        # Build mapping from query file to times
        tikv_times = {query: time for query, time in tikv_successful}
        tiflash_times = {query: time for query, time in tiflash_successful}

        # Collect all queries that succeeded in at least one engine
        # all_queries = set(list(tikv_times.keys()) + list(tiflash_times.keys()))

        # Create ASCII table for TiKV results
        logger.info("\nTiKV Benchmark Results:")
        self._print_engine_table(tikv_times)

        # Create ASCII table for TiFlash results
        logger.info("\nTiFlash Benchmark Results:")
        self._print_engine_table(tiflash_times)

        # Create comparison table
        logger.info("\nPerformance Comparison:")
        self._print_comparison_table(tikv_times, tiflash_times)

    def _print_engine_table(self, times: Dict[str, float]) -> None:
        """Print ASCII table for a single engine's results."""
        if not times:
            logger.info("No successful queries")
            return

        headers = ["Query", "Time (seconds)"]
        table = PrettyTable()
        table.field_names = headers
        table.align = "l"
        table.align["Time (seconds)"] = "r"
        for query, time in sorted(times.items()):
            table.add_row([query, f"{time:.2f}"])
        logger.info(table)

    def _print_comparison_table(
        self, tikv_times: Dict[str, float], tiflash_times: Dict[str, float]
    ) -> None:
        """Print ASCII comparison table."""
        headers = [
            "Query",
            "TiKV (s)",
            "TiFlash (s)",
            "Improvement %",
            "Speedup Factor",
        ]
        table = PrettyTable()
        table.field_names = headers
        table.align = "l"
        table.align["TiKV (s)"] = "r"
        table.align["TiFlash (s)"] = "r"
        table.align["Improvement %"] = "r"
        table.align["Speedup Factor"] = "r"

        total_tikv = 0.0
        total_tiflash = 0.0
        comparable_count = 0

        for query in sorted(set(list(tikv_times.keys()) + list(tiflash_times.keys()))):
            tikv_time = tikv_times.get(query)
            tiflash_time = tiflash_times.get(query)

            if tikv_time is None or tiflash_time is None:
                # Skip queries that failed in one engine
                continue

            improvement = (
                ((tikv_time - tiflash_time) / tikv_time) * 100 if tikv_time > 0 else 0
            )
            speedup = tikv_time / tiflash_time if tiflash_time > 0 else 0

            table.add_row(
                [
                    query,
                    f"{tikv_time:.2f}",
                    f"{tiflash_time:.2f}",
                    f"{improvement:.2f}%",
                    f"{speedup:.2f}x",
                ]
            )
            total_tikv += tikv_time
            total_tiflash += tiflash_time
            comparable_count += 1

        if comparable_count > 0:
            # Add total row
            total_improvement = (
                ((total_tikv - total_tiflash) / total_tikv) * 100
                if total_tikv > 0
                else 0
            )
            total_speedup = total_tikv / total_tiflash if total_tiflash > 0 else 0
            table.add_row(
                [
                    "TOTAL",
                    f"{total_tikv:.2f}",
                    f"{total_tiflash:.2f}",
                    f"{total_improvement:.2f}%",
                    f"{total_speedup:.2f}x",
                ]
            )
            logger.info(table)
        else:
            logger.info("No comparable queries (both engines succeeded)")

    def _restart_with_default_user(
        self, sql_files: List[str], queries_folder: str
    ) -> None:
        """Restart benchmark with default user"""
        self.db_config["user"] = "default"

        successful_queries = []
        failed_queries = []
        total_time = 0

        with self.connection():
            for sql_file in sql_files:
                logger.info(f"\n[PROC] Processing {sql_file} (with default user)...")
                file_path = os.path.join(queries_folder, sql_file)

                try:
                    with open(file_path, "r", encoding="utf-8") as file:
                        sql_script = file.read()

                    success, query_time = self.execute_query_with_retry(
                        sql_script, sql_file
                    )

                    if success:
                        successful_queries.append((sql_file, query_time))
                        total_time += query_time
                    else:
                        failed_queries.append(sql_file)

                except Exception as err:
                    logger.error(
                        f"[FAIL] Error executing {sql_file} with default user: {err}"
                    )
                    failed_queries.append(sql_file)

        self._print_summary(successful_queries, failed_queries, total_time)

        if failed_queries:
            sys.exit(1)

    def _print_summary(
        self,
        successful_queries: List[Tuple[str, float]],
        failed_queries: List[str],
        total_time: float,
    ) -> None:
        """Print benchmark summary"""
        total_queries = len(successful_queries) + len(failed_queries)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"[SUMMARY] BENCHMARK SUMMARY ({self.database_type.upper()})")
        logger.info(f"{'=' * 60}")
        logger.info(
            f"[OK] Successful queries: {len(successful_queries)}/{total_queries}"
        )
        logger.info(f"[FAIL] Failed queries: {len(failed_queries)}")

        if total_time > 0:
            minutes, seconds = divmod(total_time, 60)
            logger.info(f"[TIME] Total execution time: {int(minutes)}m {seconds:.2f}s")

        if successful_queries:
            logger.info(f"\n[OK] SUCCESSFUL QUERIES:")
            for query, exec_time in successful_queries:
                minutes, seconds = divmod(exec_time, 60)
                logger.info(f"  {query}: {int(minutes)}m {seconds:.2f}s")

        if failed_queries:
            logger.info(f"\n[FAIL] FAILED QUERIES:")
            for query in failed_queries:
                logger.info(f"  {query}")


def main():
    parser = argparse.ArgumentParser(description="Run database benchmarks")
    parser.add_argument(
        "--database",
        action=DatabaseChoiceAction,
        required=True,
        help="Specify which database to connect to",
    )
    parser.add_argument(
        "--host", help="Database host (overrides default for selected database)"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Database port (overrides default for selected database)",
    )
    parser.add_argument(
        "--user", help="Database user (overrides default for selected database)"
    )
    parser.add_argument(
        "--password", help="Database password (overrides default for selected database)"
    )
    parser.add_argument(
        "--queries-folder", default="queries/sql", help="Path to SQL queries folder"
    )
    parser.add_argument(
        "--engine",
        choices=["tikv", "tiflash", "both", "compare"],
        default="both",
        help="Storage engine for TiDB (tikv, tiflash, both, compare)",
    )

    args = parser.parse_args()

    # Prepare configuration overrides
    config_overrides = {}
    if args.host:
        config_overrides["host"] = args.host
        logger.info(f"[CONFIG] Override host: {args.host}")
    if args.port:
        config_overrides["port"] = args.port
        logger.info(f"[CONFIG] Override port: {args.port}")
    if args.user:
        config_overrides["user"] = args.user
        logger.info(f"[CONFIG] Override user: {args.user}")
    if args.password:
        config_overrides["password"] = args.password
        logger.info(f"[CONFIG] Override password: ***")

    # Run benchmarks
    benchmark = DatabaseBenchmark(args.database, config_overrides, engine=args.engine)
    benchmark.run_benchmarks(args.queries_folder)


if __name__ == "__main__":
    main()
