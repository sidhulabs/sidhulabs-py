import pytest
from sqlalchemy import Column, Integer, String

from sidhulabs.database.base_sql_model import Base
from sidhulabs.database.conn import DataBaseConn


@pytest.fixture
def mock_db_uri(monkeypatch):
    def mock_get_db_uri(self):
        return "sqlite:///"

    monkeypatch.setattr(DataBaseConn, "_get_dburi", mock_get_db_uri)


@pytest.fixture
def db(mock_db_uri) -> DataBaseConn:
    db = DataBaseConn()
    setattr(db, "db_type", "sqlite")
    return db


@pytest.fixture
def create_table(db: DataBaseConn):
    stmt = "CREATE TABLE test(ID INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);"  # noqa: E501
    db.execute(stmt)
    yield
    stmt = "DROP TABLE test;"
    db.execute(stmt)


def test_table_not_exists(db: DataBaseConn):
    """Test return True if table does not exist."""
    assert not db.table_exists("foo")


def test_table_exists(db: DataBaseConn, create_table):
    """Test return True if table exists."""
    assert db.table_exists("test")


def test_execute_select(db: DataBaseConn, create_table):
    """Test running a select query."""
    with pytest.raises(StopIteration):
        res = db.execute("SELECT * FROM test")
        next(res)


def test_insert_from_dict(db: DataBaseConn, create_table):
    "Test inserting data from a dict."
    data = {"name": "sidhulabs"}

    db.insert_from_dict("test", data)

    res = db.execute("SELECT * FROM test")

    assert next(res) == (1, "sidhulabs")


def test_insert_from_dict_table(db: DataBaseConn, create_table):
    """Test inserting data from a dict using SqlAlchemy table."""

    class TestTable(Base):
        __tablename__ = "test"

        id = Column(Integer, primary_key=True)
        test = Column(String(256), nullable=False)

    data = {"name": "foo"}

    db.insert_from_dict(TestTable, data)

    res = db.execute("SELECT * FROM test")

    assert next(res) == (1, "foo")


def test_insert_from_dict_table_create(db: DataBaseConn):
    """Test inserting data from a dict using SqlAlchemy table but table does not exist."""  # noqa: E501

    class MockTable(Base):
        __tablename__ = "mocktable"

        id = Column(Integer, primary_key=True)
        mock = Column(String(256), nullable=False)

    data = {"mock": "foobar"}

    db.insert_from_dict(MockTable, data)

    res = db.execute("SELECT * FROM mocktable")

    assert next(res) == (1, "foobar")


def test_insert_from_dict_table_error(db: DataBaseConn):
    """Test inserting data from a dict not using SqlAlchemy table but table does not exist."""  # noqa: E501
    data = {"mock": "foobar"}

    with pytest.raises(ValueError):
        db.insert_from_dict("nonexistent_table", data)
