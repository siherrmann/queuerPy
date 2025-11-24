"""
Master database handler for Python queuer implementation.
Mirrors Go's database/dbMaster.go with psycopg3 and references same SQL functions.
"""

import json
from typing import Optional
from psycopg.rows import dict_row

from ..helper.database import Database
from ..helper.sql import SQLLoader, run_ddl
from ..model.master import Master, MasterSettings
from ..model.worker import Worker


class MasterDBHandler:
    """
    Master database handler.
    Mirrors Go's MasterDBHandler with psycopg3.
    """

    def __init__(self, db_connection: Database, with_table_drop: bool = False):
        """
        Initialize master database handler.
        Mirrors Go's NewMasterDBHandler method.
        """
        self.db: Database = db_connection
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        sql_loader: SQLLoader = SQLLoader()
        sql_loader.load_master_sql(self.db.instance, force=with_table_drop)

        if with_table_drop:
            self.drop_table()

        self.create_table()

    def check_table_existence(self) -> bool:
        """Check if master table exists. Mirrors Go's CheckTableExistance method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'master'
                );
                """
            )
            result = cur.fetchone()
            return bool(result[0]) if result else False

    def create_table(self) -> None:
        """Create master table. Mirrors Go's CreateTable method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor() as cur:
            cur.execute("SELECT init_master();")
        self.db.instance.commit()

    def drop_table(self) -> None:
        """Drop master table. Mirrors Go's DropTable method."""
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        run_ddl(self.db.instance, "DROP TABLE IF EXISTS master CASCADE;")

    def select_master(self) -> Master:
        """
        Select the master record.
        Mirrors Go's SelectMaster method.

        :return: Master instance or None if not found
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        with self.db.instance.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM select_master();")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Master record not found")

            return Master.from_row(row)

    def update_master(
        self, worker: Worker, master_settings: MasterSettings
    ) -> Optional[Master]:
        """
        Update master record with worker and settings.
        Mirrors Go's UpdateMaster method.

        :param worker: Worker instance that should become master
        :param master_settings: Master settings to apply
        :return: Updated Master instance or None
        """
        if self.db.instance is None:
            raise ValueError("Database connection is not established")

        try:
            settings_json = json.dumps(
                {
                    "retention_archive": master_settings.retention_archive,
                    "master_poll_interval": master_settings.master_poll_interval,
                    "lock_timeout_minutes": master_settings.lock_timeout_minutes,
                }
            )

            with self.db.instance.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT * FROM update_master(%s, %s, %s, %s);",
                    (
                        worker.id,
                        worker.rid,
                        settings_json,
                        master_settings.lock_timeout_minutes,
                    ),
                )
                row = cur.fetchone()
                if row:
                    master = Master.from_row(row)
                    self.db.instance.commit()
                    return master
                else:
                    return None

        except Exception as e:
            self.db.instance.rollback()
            raise e
