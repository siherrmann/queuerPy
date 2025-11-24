"""
Database helper functions for Python queuer implementation.
Mirrors Go's helper/database.go with Python database utilities.
"""

import os
import logging
from typing import Optional, Dict
from dataclasses import dataclass
import psycopg
from psycopg import Connection, ConnectionInfo, sql

from .logging import QueuerLogger
from .error import QueuerError
from .sql import run_ddl


@dataclass
class DatabaseConfiguration:
    """
    Database configuration class.
    Mirrors Go's DatabaseConfiguration struct.
    """

    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str = "public"
    sslmode: str = "require"
    with_table_drop: bool = False

    @classmethod
    def from_env(cls) -> "DatabaseConfiguration":
        """Create configuration from environment variables."""
        # Get required environment variables
        host = os.getenv("QUEUER_DB_HOST", "localhost")
        port = int(os.getenv("QUEUER_DB_PORT", "5432"))
        database = os.getenv("QUEUER_DB_DATABASE", "queuer")
        username = os.getenv("QUEUER_DB_USERNAME", "postgres")
        password = os.getenv("QUEUER_DB_PASSWORD", "")
        schema = os.getenv("QUEUER_DB_SCHEMA", "public")
        sslmode = os.getenv("QUEUER_DB_SSLMODE", "require")
        with_table_drop = (
            os.getenv("QUEUER_DB_WITH_TABLE_DROP", "false").lower() == "true"
        )

        # Validate required fields
        if not all(
            [
                host.strip(),
                str(port),
                database.strip(),
                username.strip(),
                schema.strip(),
            ]
        ):
            raise ValueError(
                "Required environment variables missing: "
                "QUEUER_DB_HOST, QUEUER_DB_PORT, QUEUER_DB_DATABASE, "
                "QUEUER_DB_USERNAME, QUEUER_DB_SCHEMA must be set",
                "Missing required environment variables",
            )

        return cls(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            sslmode=sslmode,
            with_table_drop=with_table_drop,
        )

    def connection_string(self) -> str:
        """Get connection string for psycopg3."""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.username} "
            f"password={self.password} "
            f"sslmode={self.sslmode} "
            f"application_name=queuer "
            f"options='-c search_path={self.schema}'"
        )


class Database:
    """
    Database service wrapper.
    Mirrors Go's Database struct with Python database connections.
    """

    def __init__(
        self,
        name: str,
        config: Optional[DatabaseConfiguration] = None,
        logger: Optional[QueuerLogger] = None,
        auto_connect: bool = True,
    ):
        """Initialize database service."""
        self.name = name
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.instance: Optional[Connection] = None

        if config and auto_connect:
            self.connect_to_database()

    def connect_to_database(self) -> None:
        """
        Connect to the database using the configuration.
        Mirrors Go's ConnectToDatabase method.
        """
        if not self.config:
            raise QueuerError(
                "Database configuration is required for connection",
                ValueError("No config provided"),
            )

        try:
            # Create connection
            self.instance = psycopg.connect(
                self.config.connection_string(), autocommit=False
            )

            # Set connection parameters
            self.instance.execute("SET application_name = 'queuer'")

            # Create pg_trgm extension if it doesn't exist
            try:
                run_ddl(self.instance, "CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            except Exception as e:
                self.logger.warning(f"Could not create pg_trgm extension: {e}")

            # Test connection
            self.instance.execute("SELECT 1")
            self.logger.info(f"Connected to database: {self.config.database}")

        except Exception as e:
            raise QueuerError("Failed to connect to database", e)

    def check_table_existence(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        Mirrors Go's CheckTableExistance method.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        try:
            with self.instance.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = current_schema()
                        AND table_name = %s
                    );
                """,
                    (table_name,),
                )

                result = cur.fetchone()
                return result[0] if result else False

        except Exception as e:
            raise QueuerError(f"Failed to check table existence for {table_name}", e)

    def create_index(self, table_name: str, column_name: str) -> None:
        """
        Create an index on the specified column of the specified table.
        Mirrors Go's CreateIndex method.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        try:
            index_name = f"idx_{table_name}_{column_name}"
            create_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(table_name),
                sql.Identifier(column_name),
            )
            run_ddl(self.instance, create_sql.as_string(self.instance))
            self.logger.info(f"Created index {index_name}")

        except Exception as e:
            raise QueuerError(
                f"Failed to create index on {table_name}.{column_name}", e
            )

    def create_indexes(self, table_name: str, *column_names: str) -> None:
        """
        Create indexes on the specified columns of the specified table.
        Mirrors Go's CreateIndexes method.
        """
        for column_name in column_names:
            self.create_index(table_name, column_name)

    def create_combined_index(
        self, table_name: str, column_name1: str, column_name2: str
    ) -> None:
        """
        Create a combined index on the specified columns of the specified table.
        Mirrors Go's CreateCombinedIndex method.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        try:
            index_name = f"idx_{table_name}_{column_name1}_{column_name2}"
            create_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({}, {})").format(
                sql.Identifier(index_name),
                sql.Identifier(table_name),
                sql.Identifier(column_name1),
                sql.Identifier(column_name2),
            )
            run_ddl(self.instance, create_sql.as_string(self.instance))
            self.logger.info(f"Created combined index {index_name}")

        except Exception as e:
            raise QueuerError(
                f"Failed to create combined index on {table_name}.{column_name1},{column_name2}",
                e,
            )

    def create_unique_combined_index(
        self, table_name: str, column_name1: str, column_name2: str
    ) -> None:
        """
        Create a unique combined index on the specified columns of the specified table.
        Mirrors Go's CreateUniqueCombinedIndex method.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        try:
            index_name = f"idx_{table_name}_{column_name1}_{column_name2}"
            with self.instance.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                    CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({}, {})
                """
                    ).format(
                        sql.Identifier(index_name),
                        sql.Identifier(table_name),
                        sql.Identifier(column_name1),
                        sql.Identifier(column_name2),
                    )
                )
            self.instance.commit()
            self.logger.info(f"Created unique combined index {index_name}")

        except Exception as e:
            self.instance.rollback()
            raise QueuerError(
                f"Failed to create unique combined index on {table_name}.{column_name1},{column_name2}",
                e,
            )

    def drop_index(self, table_name: str, json_map_key: str) -> None:
        """
        Drop the index on the specified table and column.
        Mirrors Go's DropIndex method.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        index_name = f"idx_{table_name}_{json_map_key}"
        try:
            drop_sql = sql.SQL("DROP INDEX IF EXISTS {}").format(
                sql.Identifier(index_name)
            )
            run_ddl(self.instance, drop_sql.as_string(self.instance))
            self.logger.info(f"Dropped index {index_name}")

        except Exception as e:
            raise QueuerError(f"Failed to drop index {index_name}", e)

    def health(self) -> Dict[str, str]:
        """
        Check the health of the database connection.
        Mirrors Go's Health method.
        """
        stats: Dict[str, str] = {}

        if not self.instance:
            stats["status"] = "down"
            stats["error"] = "No database connection"
            return stats

        try:
            # Test connection with simple query
            with self.instance.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

            stats["status"] = "up"
            stats["message"] = "It's healthy"

            # Get connection info
            info: ConnectionInfo = self.instance.info
            stats["server_version"] = str(info.server_version)
            stats["backend_pid"] = str(info.backend_pid)

            # Get transaction status
            status_map = {
                psycopg.pq.TransactionStatus.IDLE: "idle",
                psycopg.pq.TransactionStatus.ACTIVE: "active",
                psycopg.pq.TransactionStatus.INTRANS: "in_transaction",
                psycopg.pq.TransactionStatus.INERROR: "in_error",
                psycopg.pq.TransactionStatus.UNKNOWN: "unknown",
            }
            stats["transaction_status"] = status_map.get(
                info.transaction_status, "unknown"
            )

        except Exception as e:
            stats["status"] = "down"
            stats["error"] = f"Database health check failed: {str(e)}"
            self.logger.error(f"Database health check failed: {e}")

        return stats

    def create_table_if_not_exists(self, table_name: str, create_sql: str) -> bool:
        """
        Create a table if it doesn't exist.
        Returns True if table was created, False if it already existed.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        if self.check_table_existence(table_name):
            return False

        try:
            run_ddl(self.instance, create_sql)
            self.logger.info(f"Created table: {table_name}")
            return True

        except Exception as e:
            raise QueuerError(f"Failed to create table {table_name}", e)

    def drop_table_if_exists(self, table_name: str) -> bool:
        """
        Drop a table if it exists.
        Returns True if table was dropped, False if it didn't exist.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        if not self.check_table_existence(table_name):
            return False

        try:
            run_ddl(self.instance, f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            self.logger.info(f"Dropped table: {table_name}")
            return True

        except Exception as e:
            raise QueuerError(f"Failed to drop table {table_name}", e)

    def execute_sql_file(self, file_path: str) -> None:
        """
        Execute SQL commands from a file.
        Handles multiple statements separated by semicolons.
        """
        if not self.instance:
            raise Exception("Database connection not established")

        try:
            with open(file_path, "r") as f:
                sql_content = f.read()

            # Split and execute statements
            statements = [
                stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
            ]

            with self.instance.cursor() as cur:
                for statement in statements:
                    if statement:
                        cur.execute(statement.encode("utf-8"))

            self.instance.commit()
            self.logger.info(f"Executed SQL file: {file_path}")

        except Exception as e:
            self.instance.rollback()
            raise QueuerError(f"Failed to execute SQL file {file_path}", e)

    def close(self) -> None:
        """Close the database connection."""
        if self.instance:
            self.instance.close()
            self.instance = None
            self.logger.info("Database connection closed")


def new_database(
    name: str,
    config: DatabaseConfiguration,
    logger: Optional[QueuerLogger] = None,
    auto_connect: bool = True,
) -> Database:
    """
    Create a new Database instance.
    Mirrors Go's NewDatabase function.
    """
    return Database(name, config, logger, auto_connect)


def new_database_from_env(
    name: str = "queuer",
    logger: Optional[QueuerLogger] = None,
    auto_connect: bool = False,
) -> Database:
    """
    Create a new Database instance from environment variables.
    Mirrors Go's helper database creation pattern.
    """
    config = DatabaseConfiguration.from_env()
    return Database(name, config, logger, auto_connect)


def new_database_with_connection(
    name: str, connection: Connection, logger: Optional[QueuerLogger] = None
) -> Database:
    """
    Create a new Database instance with an existing connection.
    Mirrors Go's NewDatabaseWithDB function.
    """
    db = Database(name, None, logger)
    db.instance = connection
    return db


# Database utility functions
def validate_connection_string(connection_string: str) -> bool:
    """Validate a PostgreSQL connection string format."""
    try:
        # Try to parse the connection string
        psycopg.connect(connection_string, connect_timeout=1)
        return True
    except Exception:
        return False


def get_database_version(db: Database) -> Optional[str]:
    """Get the PostgreSQL server version."""
    if not db.instance:
        return None

    try:
        with db.instance.cursor() as cur:
            cur.execute("SELECT version();")
            result = cur.fetchone()
            return result[0] if result else None
    except Exception:
        return None


def check_extension_exists(db: Database, extension_name: str) -> bool:
    """Check if a PostgreSQL extension is installed."""
    if not db.instance:
        return False

    try:
        with db.instance.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_extension 
                    WHERE extname = %s
                );
            """,
                (extension_name,),
            )
            result = cur.fetchone()
            return result[0] if result else False
    except Exception:
        return False
