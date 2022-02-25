import os
import time
import urllib
from contextlib import contextmanager
from textwrap import dedent
from typing import Tuple, Union

import sqlalchemy
from loguru import logger
from sqlalchemy.orm import sessionmaker

from sidhulabs.database.base_sql_model import Base
from sidhulabs.decorators import Substitution, doc

POSTGRES = "postgresql"
SQLITE = "sqlite"
MSSQL = "mssql"
DATABASE_ENGINES = [POSTGRES, SQLITE, MSSQL]
SQLALCHEMYSTORE_POOL_SIZE = "SQLALCHEMYSTORE_POOL_SIZE"
SQLALCHEMYSTORE_MAX_OVERFLOW = "SQLALCHEMYSTORE_MAX_OVERFLOW"
MAX_RETRY_COUNT = 15

DB_CONN_DOCSTRING = dedent(
    """
    Interface to interact with a {db} Database.

    Examples
    --------
    >>> from sidhulabs.database import {conn}

    >>> # Connect to a specific database
    >>> db = {conn}(host="my-hostname.com", db="my-database", user="sidhulabs", password="sidhulabs_password")
    Successfully connected to Database my-database at my-hostname.com!

    >>> # Check if table exists
    >>> db.table_exists("dbo.test")
    True

    >>> # Insert data into table from a dictionary
    >>> data = {{
    ...     "name": "sidhulabs",
    ...     "money": 0.0,
    ...     "num": 3
    ... }}
    >>> db.insert_from_dict("dbo.test_table", data)
    Data inserted successfully!

    >>> # Run your own query
    >>> res = db.execute("SELECT * FROM table_name")
    >>> for r in res:
    ...     print(r)
    (1, 2, 3)
    (4, 5, 6)

    >>> # Load data into pandas
    >>> import pandas as pd
    >>> df = pd.read_sql("SELECT * FROM dbo.test_table, db.engine.connect())

    >>> # Altering a table requires a commit
    >>> db.execute("DROP TABLE dbo.test_table")
    """
)


class DataBaseConn(object):
    def __init__(
        self,
        host: str = None,
        db: str = None,
        user: str = None,
        password: str = None,
        port: str = None,
        isolation_level: str = None,  # none means default
    ):
        """
        NOTE : valid values for isolation level include:
        * READ COMMITTED
        * READ UNCOMMITTED
        * REPEATABLE READ
        * SERIALIZABLE
        * SNAPSHOT - specific to SQL Server
        Make sure you know what you are doing before you alter
        the isolation level from its default.
        """

        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self.isolation_level = isolation_level
        self.db_uri = self._get_dburi()
        self.engine = self._create_sqlalchemy_engine_with_retry()

        self.SessionMaker = sessionmaker(bind=self.engine)
        self.session = self.SessionMaker()

        self.ManagedSessionMaker = self._get_managed_session_maker(self.session)

        Base.metadata.bind = self.engine

        logger.info(f"Successfully connected to Database {self.db} at {self.host}!")

    def execute(self, statement: str):
        """
        Executes SQL Statements

        Parameters
        ----------
        statement : str
            SQL Statement

        Returns
        -------
        ResultSet
            ResultSet generator for any query results.

        Examples
        --------
        >>> res = sqlc.execute("SELECT * FROM table_name")
        >>> for r in res:
        ...     print(r)
        (1, 2, 3)
        (4, 5, 6)
        """

        command = statement.split()[0].lower()

        if command == "select":
            res = self.session.execute(statement)
        else:
            with self.ManagedSessionMaker() as db:
                res = db.execute(statement)

        return res

    def insert_from_dict(self, table: Union[str, sqlalchemy.ext.declarative.DeclarativeMeta], data: dict):
        """
        Inserts data into a table from a dictionary.

        Parameters
        ----------
        table : str or SqlAlchemy table
            Table name

        data : dict
            Data to insert into the table

        Examples
        --------
        >>> from sidhulabs.database import %(conn)s

        >>> sqlc = %(conn)s()
        >>> data = {
        ...     "name": "sidhulabs",
        ...     "money": 0.0,
        ...     "num": 3
        ... }
        >>> sqlc.insert_from_dict("dbo.test_table", data)
        Data inserted successfully!
        """

        if isinstance(table, sqlalchemy.ext.declarative.DeclarativeMeta):
            table_name = table.__tablename__
            schema = table.__table_args__.get("schema", None) if hasattr(table, "__table_args__") else None
        else:
            schema, table_name = self._get_table_name_schema(table)

        if schema:
            self._create_schema(schema)

        if not self.table_exists(table_name, schema):
            if isinstance(table, sqlalchemy.ext.declarative.DeclarativeMeta):
                self._create_tables()
            else:
                raise ValueError(f"{table_name} does not exist!")

        tbl = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), schema=schema, autoload_with=self.engine)
        ins = tbl.insert().values(**data)

        with self.ManagedSessionMaker() as db:
            db.execute(ins)

        logger.info("Data inserted successfully!")

    def table_exists(self, table: str, schema=None) -> bool:
        """
        Checks if a table exists in the database.

        Parameters
        ----------
        table : str
            Table name to check.

        Returns
        -------
        bool
            True if the table exists in the database.

        Examples
        --------
        >>> from sidhulabs.database import %(conn)s

        >>> sqlc = %(conn)s()
        >>> sqlc.table_exists("test_table")
        >>> sqlc.table_exists("test_table", "dbo")
        """

        return table in sqlalchemy.inspect(self.engine).get_table_names(schema=schema)

    def _save_to_db(self, session, objs):
        """
        Stores objects in db.
        """
        if type(objs) is list:
            session.add_all(objs)
        else:
            # single object
            session.add(objs)

    def _get_managed_session_maker(self, session):
        """
        Creates a factory for producing exception-safe SQLAlchemy sessions that are made available
        using a context manager. Any session produced by this factory is automatically committed
        if no exceptions are encountered within its associated context. If an exception is
        encountered, the session is rolled back. Finally, any session produced by this factory is
        automatically closed when the session's associated context is exited.
        """

        @contextmanager
        def make_managed_session():
            """Provide a transactional scope around a series of operations."""
            try:
                if self.db_type == SQLITE:
                    session.execute("PRAGMA foreign_keys = ON;")
                    session.execute("PRAGMA case_sensitive_like = true;")
                yield session
                session.commit()
            except Exception as e:
                session.rollback()
                raise Exception(e) from e
            finally:
                session.close()

        return make_managed_session

    def _create_sqlalchemy_engine_with_retry(self):
        """
        Tries to create a SQL Alchemy engine
        """

        attempts = 0
        while True:
            attempts += 1
            engine = self._create_sqlalchemy_engine()
            try:
                sqlalchemy.inspect(engine)
                return engine
            except Exception as e:
                if attempts < MAX_RETRY_COUNT:
                    sleep_duration = 0.1 * ((2 ** attempts) - 1)
                    logger.warning(
                        "SQLAlchemy engine could not be created. The following exception is caught.\n"
                        f"{e}\nOperation will be retried in {sleep_duration:.1f} seconds",
                        e,
                        sleep_duration,
                    )
                    time.sleep(sleep_duration)
                    continue
                raise

    def _create_sqlalchemy_engine(self):
        """
        Creates a SQL Alchemy engine.
        """

        pool_size = os.environ.get(SQLALCHEMYSTORE_POOL_SIZE, 3)
        pool_max_overflow = os.environ.get(SQLALCHEMYSTORE_MAX_OVERFLOW, 0)
        pool_kwargs = {}
        # Send argument only if they have been injected.
        # Some engine does not support them (for example sqllite)
        if pool_size:
            pool_kwargs["pool_size"] = int(pool_size)
        if pool_max_overflow:
            pool_kwargs["max_overflow"] = int(pool_max_overflow)
        if self.isolation_level:
            pool_kwargs["isolation_level"] = str(self.isolation_level)
        if pool_kwargs:
            logger.info(f"Create SQLAlchemy engine with pool options {pool_kwargs}")

        # logging options
        echo = os.getenv("SQLALCHEMY_ECHO") or False  # e.g. debug
        if echo == "None":  # for easily toggling back without purging env var
            echo = False

        return sqlalchemy.create_engine(self.db_uri, pool_pre_ping=True, echo=echo, **pool_kwargs)

    def _create_tables(self):
        """
        Creates all tables declared via Base
        """

        try:
            Base.metadata.create_all(self.engine)
        except sqlalchemy.exc.ProgrammingError as e:
            if "schema name" in str(e):
                logger.warning("Could not create all the tables due to missing schemas")
            else:
                raise e from e

        # TODO: Add alembic table logic

    def _create_schema(self, schema_name):
        """
        Creates db schema if it does not exist.
        """

        if self.db_type != SQLITE:
            if schema_name not in self.engine.dialect.get_schema_names(self.engine):
                self.engine.execute(sqlalchemy.schema.CreateSchema(schema_name))

    def _get_table_name_schema(self, table: str) -> Tuple[str, str]:
        """
        Gets the table name and schema from a table string.
        """

        table_split = table.split(".")

        schema, name = (None, table) if len(table_split) != 2 else table_split
        return schema, name

    def _get_dburi(self):
        """
        Abstract method, may move to abstract class.
        """
        pass


@doc(DB_CONN_DOCSTRING, db="Postgres", conn="PostgresConn")
class PostgresConn(DataBaseConn):  # pragma: no cover

    class_name = "PostgresConn"

    def __init__(
        self,
        host: str = None,
        db: str = None,
        user: str = None,
        password: str = None,
        port: str = "5432",
        isolation_level: str = None,  # none means default
    ):
        self.db_type = "postgresql"
        DataBaseConn.__init__(self, host, db, user, password, port, isolation_level)

    def _get_dburi(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    insert_from_dict = Substitution(conn=class_name)(DataBaseConn.insert_from_dict)
    table_exists = Substitution(conn=class_name)(DataBaseConn.table_exists)


@doc(DB_CONN_DOCSTRING, db="MSFT SQL Server", conn="SqlServerConn")
class SqlServerConn(DataBaseConn):  # pragma: no cover

    class_name = "SQLServerConn"

    def __init__(
        self,
        host: str = None,
        db: str = None,
        user: str = None,
        password: str = None,
        port: str = "5432",
        isolation_level: str = None,  # none means default
    ):
        self.db_type = "mssql"
        DataBaseConn.__init__(self, host, db, user, password, port, isolation_level)
        self.url = f"jdbc:sqlserver://{self.host};databaseName={self.db};"

    def _get_dburi(self):
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + self.host
            + ";DATABASE="
            + self.db
            + ";UID="
            + self.user
            + ";PWD="
            + self.password
        )

        return f"mssql+pyodbc:///?odbc_connect={params}"

    insert_from_dict = Substitution(conn=class_name)(DataBaseConn.insert_from_dict)
    table_exists = Substitution(conn=class_name)(DataBaseConn.table_exists)


@doc(DB_CONN_DOCSTRING, db="Sqlite", conn="SqliteConn")
class SqliteConn(DataBaseConn):

    class_name = "SqlLiteConn"

    def __init__(
        self,
        db: str = "/",
    ):
        self.db_type = "sqllite"
        DataBaseConn.__init__(self, "", db, "", "", "", None)
        self.url = ""

    def _get_dburi(self):
        return f"sqlite://{self.db}"

    insert_from_dict = Substitution(conn=class_name)(DataBaseConn.insert_from_dict)
    table_exists = Substitution(conn=class_name)(DataBaseConn.table_exists)
