import pytest
import sqlite3
from contextlib import closing
from nvi_gpkg_validator.nvi_gpkg import (
    NviForeignKey,
    NviCheckConstraint,
    NviColumn,
    NviGPKG,
    NviTable,
)


@pytest.fixture
def table_columns():
    return [
        NviColumn("col1", "int", True, True, True),
        NviColumn("col2", "text"),
        NviColumn("col3", "int"),
    ]


@pytest.fixture
def ref_table_columns():
    return [NviColumn("ref_column", "int")]


@pytest.fixture
def table_fk_def():
    return NviForeignKey("col3", "ref_table", "ref_column")


@pytest.fixture
def main_table(table_columns, table_fk_def):
    return NviTable("tablename", table_columns, [table_fk_def])


@pytest.fixture
def ref_table(ref_table_columns):
    return NviTable("ref_table", ref_table_columns)


@pytest.fixture
def db_tables(ref_table, main_table):
    db_tables = [ref_table, main_table]
    return db_tables


def test_nvi_check_constraint_sql():
    check_sql = NviCheckConstraint("constraint").sql()
    assert check_sql == "CHECK(constraint)"


def test_nvi_foreign_key_sql():
    fk_sql = NviForeignKey("column", "ref_table", "ref_column").sql()
    assert fk_sql == "FOREIGN KEY (column) REFERENCES ref_table(ref_column)"


def test_nvi_column_sql():
    col = NviColumn("colname", "int", True, True, True)
    col_sql = col.sql()
    assert col_sql == "colname int NOT NULL PRIMARY KEY UNIQUE"


class TestNviTable:
    def test_table_sql(self, main_table):
        sqldef = "CREATE TABLE tablename (col1 int NOT NULL PRIMARY KEY UNIQUE, col2 text, col3 int, FOREIGN KEY (col3) REFERENCES ref_table(ref_column))"
        assert main_table.sql() == sqldef

    def test_column_by_name(self, main_table):
        column = main_table.column_by_name("col1")
        assert column.name == "col1"
        assert column.datatype == "int"
        assert column.notnull
        assert column.pk
        assert column.unique

    def test_set_column_notnull(self, main_table):
        main_table.set_column_notnull("col2")
        assert main_table.column_by_name("col2").notnull
        assert main_table.list_notnull_columns() == ["col1", "col2"]

    def test_add_check_constraint(self, main_table):
        main_table.add_check_constraint("col3 > 0")
        new_constraint = [
            i for i in main_table.check_constraints if i.constraint == "col3 > 0"
        ]
        assert len(new_constraint) > 0

    def test_add_foreign_key(self, main_table):
        col1_fk = [i for i in main_table.foreign_keys if i.column == "col1"]
        assert len(col1_fk) == 0
        main_table.add_foreign_key("col1", "ref_table", "ref_column")
        col1_fk = [i for i in main_table.foreign_keys if i.column == "col1"]
        assert len(col1_fk) > 0

    def test_create_and_read_table(self, tmp_path, db_tables):
        db = tmp_path / "testdb.gpkg"
        # create tables
        with closing(sqlite3.Connection(db)) as con:
            for table in db_tables:
                con.execute(table.sql())
        # read table by name
        read_table = NviTable.from_gpkg(db, "tablename")
        # check table data
        assert read_table.name == "tablename"
        assert read_table.list_column_names() == ["col1", "col2", "col3"]
        assert read_table.foreign_keys == [
            NviForeignKey("col3", "ref_table", "ref_column")
        ]
        assert read_table.list_notnull_columns() == ["col1"]
        assert read_table.column_by_name("col1").name == "col1"


class TestNviGPKG:
    def test_read_and_copy(self, tmp_path, db_tables):
        db = tmp_path / "testdb.gpkg"
        # create tables
        with closing(sqlite3.Connection(db)) as con:
            for table in db_tables:
                con.execute(table.sql())
        # read table names
        assert set(NviGPKG.read_table_names_from_gpkg_file(db)) == set(
            ["ref_table", "tablename"]
        )
        # read db to nvi gpkg
        nvi_gpkg_read = NviGPKG.create_from_gpkg_file(db)
        # create copy
        nvi_gpkg_copy = nvi_gpkg_read.create_copy(tmp_path, new_name="copy")
        # check file exists
        assert nvi_gpkg_copy.file_path.is_file()
        # list all tables in copy
        assert set([i.name for i in nvi_gpkg_copy.tables]) == set(
            ["ref_table", "tablename"]
        )

    def test_table_by_name(self, tmp_path, db_tables):
        nvi_gpkg = NviGPKG(tmp_path / "db.gpkg", db_tables)
        # get table by name
        assert nvi_gpkg.table_by_name("tablename").name == "tablename"
