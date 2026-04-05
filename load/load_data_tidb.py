#!/usr/bin/env python3

import os
import sys
import time
import logging
import subprocess
import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

# Constants
CONNECTION_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUIRED_CSV_FILES = ["bts.airlines.csv", "bts.airports.csv", "bts.flights.csv"]
DEFAULT_TIDB_PORT = 4000


@dataclass
class DatabaseConfig:
    host: str = "127.0.0.1"
    port: int = DEFAULT_TIDB_PORT
    user: str = "root"
    password: str = "9E@rq3^w2p5+t4Cd6_"
    database: str = "bts"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


def verify_csv_files() -> Path:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    csv_dir = project_root / "csv"

    if not csv_dir.exists():
        raise FileNotFoundError(
            f"CSV directory '{csv_dir}' not found. "
            "Please run 'python3 load/get_data.py' first to download the data."
        )

    missing_files = [
        file for file in REQUIRED_CSV_FILES if not (csv_dir / file).exists()
    ]

    if missing_files:
        raise FileNotFoundError(f"Missing required CSV files: {missing_files}")

    logging.getLogger(__name__).info("All required CSV files found.")
    return csv_dir


class DatabaseLoader:
    """Abstract base class for database loaders"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_database_and_tables(self) -> None:
        raise NotImplementedError

    def load_data(self, csv_dir: Path) -> None:
        raise NotImplementedError

    def test_connection(self) -> bool:
        return True


class TiDBLoader(DatabaseLoader):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)

    @contextmanager
    def _get_connection(self):
        import mysql.connector
        from mysql.connector import Error

        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                autocommit=True,
                connect_timeout=CONNECTION_TIMEOUT,
            )
            yield connection
        except Error as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()

    def test_connection(self) -> bool:
        try:
            with self._get_connection() as connection:
                return connection.is_connected()
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def wait_for_connection(self, timeout: int = CONNECTION_TIMEOUT) -> bool:
        self.logger.info("Waiting for TiDB to be ready...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.test_connection():
                self.logger.info("TiDB is ready!")
                return True
            time.sleep(RETRY_DELAY)

        self.logger.error(
            f"Timeout waiting for TiDB connection after {timeout} seconds"
        )
        return False

    def create_database_and_tables(self) -> None:
        self.logger.info("Creating TiDB database and tables...")

        sql_commands = [
            f"DROP DATABASE IF EXISTS `{self.config.database}`",
            f"CREATE DATABASE `{self.config.database}`",
            f"USE `{self.config.database}`",
            """CREATE TABLE `airlines` (
                `iata_code` varchar(2) DEFAULT NULL,
                `airline` varchar(30) DEFAULT NULL
            )""",
            """CREATE TABLE `airports` (
                `iata_code` varchar(3) DEFAULT NULL,
                `airport` varchar(80) DEFAULT NULL,
                `city` varchar(30) DEFAULT NULL,
                `state` varchar(2) DEFAULT NULL,
                `country` varchar(30) DEFAULT NULL,
                `latitude` decimal(11,4) DEFAULT NULL,
                `longitude` decimal(11,4) DEFAULT NULL
            )""",
            """CREATE TABLE `flights` (
                `year` smallint(6) DEFAULT NULL,
                `month` tinyint(4) DEFAULT NULL,
                `day` tinyint(4) DEFAULT NULL,
                `day_of_week` tinyint(4) DEFAULT NULL,
                `fl_date` date DEFAULT NULL,
                `carrier` varchar(2) DEFAULT NULL,
                `tail_num` varchar(6) DEFAULT NULL,
                `fl_num` smallint(6) DEFAULT NULL,
                `origin` varchar(5) DEFAULT NULL,
                `dest` varchar(5) DEFAULT NULL,
                `crs_dep_time` varchar(4) DEFAULT NULL,
                `dep_time` varchar(4) DEFAULT NULL,
                `dep_delay` decimal(13,2) DEFAULT NULL,
                `taxi_out` decimal(13,2) DEFAULT NULL,
                `wheels_off` varchar(4) DEFAULT NULL,
                `wheels_on` varchar(4) DEFAULT NULL,
                `taxi_in` decimal(13,2) DEFAULT NULL,
                `crs_arr_time` varchar(4) DEFAULT NULL,
                `arr_time` varchar(4) DEFAULT NULL,
                `arr_delay` decimal(13,2) DEFAULT NULL,
                `cancelled` decimal(13,2) DEFAULT NULL,
                `cancellation_code` varchar(20) DEFAULT NULL,
                `diverted` decimal(13,2) DEFAULT NULL,
                `crs_elapsed_time` decimal(13,2) DEFAULT NULL,
                `actual_elapsed_time` decimal(13,2) DEFAULT NULL,
                `air_time` decimal(13,2) DEFAULT NULL,
                `distance` decimal(13,2) DEFAULT NULL,
                `carrier_delay` decimal(13,2) DEFAULT NULL,
                `weather_delay` decimal(13,2) DEFAULT NULL,
                `nas_delay` decimal(13,2) DEFAULT NULL,
                `security_delay` decimal(13,2) DEFAULT NULL,
                `late_aircraft_delay` decimal(13,2) DEFAULT NULL
            )""",
        ]

        with self._get_connection() as connection:
            cursor = connection.cursor()
            try:
                for sql_command in sql_commands:
                    cursor.execute(sql_command)
                self.logger.info("Database and tables created successfully")
            except Exception as e:
                self.logger.error(f"Error creating database and tables: {e}")
                raise
            finally:
                cursor.close()

    def load_data(self, csv_dir: Path) -> None:
        self.logger.info("Starting data loading with TiDB Lightning...")

        config_path = Path(__file__).parent / "tidb-lightning.toml"

        if not config_path.exists():
            self.logger.error(f"{config_path} configuration file not found")
            raise FileNotFoundError(f"TiDB Lightning config not found: {config_path}")

        self.logger.info("Starting TiDB Lightning data import...")

        project_root = Path(__file__).parent.parent
        original_cwd = Path.cwd()

        try:
            os.chdir(project_root)

            start_time = time.time()
            process = subprocess.Popen(
                ["proxychains4", "-f", "proxychains4.conf", "tiup", "tidb-lightning", "-config", str(config_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
            )

            for line in process.stdout:
                self.logger.info(line.rstrip())

            return_code = process.wait()

            if return_code == 0:
                total_time = time.time() - start_time
                self.logger.info(
                    f"TiDB Lightning completed successfully in {total_time:.1f}s"
                )

                # Get row counts for each table
                try:
                    with self._get_connection() as connection:
                        cursor = connection.cursor()
                        tables = ["airlines", "airports", "flights"]
                        for table in tables:
                            cursor.execute(
                                f"SELECT COUNT(*) FROM {self.config.database}.{table}"
                            )
                            row_count = cursor.fetchone()[0]
                            self.logger.info(
                                f"Successfully loaded {row_count:,} rows into {table}"
                            )
                        cursor.close()
                except Exception as e:
                    self.logger.warning(f"Could not get row counts: {e}")

                self.logger.info("All data loaded successfully into TiDB!")
            else:
                self.logger.error(
                    f"TiDB Lightning failed with return code: {return_code}"
                )
                raise RuntimeError(f"TiDB Lightning failed with code {return_code}")

        finally:
            os.chdir(original_cwd)

    def set_tiflash_replica(self) -> None:
        tables = ["airlines", "airports", "flights"]

        try:
            with self._get_connection() as connection:
                cursor = connection.cursor()

                for table in tables:
                    self.logger.info(f"Setting TiFlash replica for {table} table...")
                    cursor.execute(
                        f"ALTER TABLE {self.config.database}.{table} SET TIFLASH REPLICA 1;"
                    )
                    self.logger.info(f"TiFlash replica set successfully for {table}")

                cursor.close()
        except Exception as e:
            self.logger.error(f"Error setting TiFlash replica: {e}")
            raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load BTS data into TiDB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip database and table creation (assumes they already exist)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="TiDB host")
    parser.add_argument("--port", type=int, default=DEFAULT_TIDB_PORT, help="TiDB port")
    parser.add_argument("--user", default="root", help="TiDB user")
    parser.add_argument(
        "--password", default="9E@rq3^w2p5+t4Cd6_", help="TiDB password"
    )
    parser.add_argument(
        "--database-name", default="bts", help="Database name to create and use"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--connection-timeout",
        type=int,
        default=CONNECTION_TIMEOUT,
        help="Timeout in seconds for database connection attempts",
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper())
    logger = setup_logging(log_level)
    logger.info("Starting TiDB data loading")

    try:
        csv_dir = verify_csv_files()

        config = DatabaseConfig(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database_name,
        )

        loader = TiDBLoader(config)

        # Wait for database to be ready
        if not loader.wait_for_connection(args.connection_timeout):
            logger.error("Could not connect to TiDB. Please ensure TiDB is running.")
            sys.exit(1)

        total_start_time = time.time()

        if not args.skip_schema:
            logger.info("Creating database and tables...")
            loader.create_database_and_tables()
        else:
            logger.info("Skipping database and table creation")

        logger.info("Starting data loading...")
        loader.load_data(csv_dir)

        logger.info("Setting TiFlash replica...")
        loader.set_tiflash_replica()

        total_time = time.time() - total_start_time
        logger.info(
            f"TiDB data loading completed successfully in {total_time:.2f} seconds"
        )

    except KeyboardInterrupt:
        logger.warning("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        if log_level == logging.DEBUG:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
