import copy
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union


@dataclass
class NviForeignKey:
    column: str
    ref_table: str
    ref_column: str

    def sql(self) -> str:
        return (
            f"FOREIGN KEY ({self.column}) "
            f"REFERENCES {self.ref_table}({self.ref_column})"
        )


@dataclass
class NviCheckConstraint:
    constraint: str

    def sql(self) -> str:
        return f"CHECK({self.constraint})"


@dataclass
class NviColumn:
    name: str
    datatype: str
    notnull: bool = False
    pk: bool = False
    unique: bool = False

    def sql(self) -> str:
        notnull = " NOT NULL" if self.notnull else ""
        pk = " PRIMARY KEY" if self.pk else ""
        unique = " UNIQUE" if self.unique else ""
        return f"{self.name} {self.datatype}{notnull}{pk}{unique}"


@dataclass
class NviTable:
    name: str
    columns: List[NviColumn]
    foreign_keys: List[NviForeignKey] = field(default_factory=list)
    check_constraints: List[NviCheckConstraint] = field(default_factory=list)

    def add_foreign_key(self, column: str, ref_table: str, ref_column: str) -> None:
        fk = NviForeignKey(column, ref_table, ref_column)
        if fk not in self.foreign_keys:
            self.foreign_keys.append(fk)

    def add_check_constraint(self, constraint: str) -> None:
        check = NviCheckConstraint(constraint)
        if check not in self.check_constraints:
            self.check_constraints.append(check)

    def column_by_name(self, colname: str) -> Optional[NviColumn]:
        matches = [column for column in self.columns if column.name == colname]
        if len(matches) > 0:
            return matches[0]
        else:
            return None

    def set_column_notnull(self, column_name: str) -> None:
        column = self.column_by_name(column_name)
        if column is not None:
            column.notnull = True

    def list_notnull_columns(self) -> List[str]:
        return [column.name for column in self.columns if column.notnull]

    def list_column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    def sql(self) -> str:
        sql = f"CREATE TABLE {self.name} ("
        sql += ", ".join([column.sql() for column in self.columns])
        if len(self.foreign_keys) > 0:
            sql += ", "
            sql += ", ".join([fk.sql() for fk in self.foreign_keys])
        if len(self.check_constraints) > 0:
            sql += ", "
            sql += ", ".join([check.sql() for check in self.check_constraints])
        sql += ")"
        return sql

    @classmethod
    def from_gpkg(cls, file_path: Union[Path, str], table: str):
        """read table definition from gpkg file"""
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"File: {file_path} does not exists")

        if file_path.suffix != ".gpkg":
            raise FileNotFoundError(f"File: {file_path} is not a GeoPackage")

        with closing(sqlite3.Connection(file_path)) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            table_info = cur.execute(f"pragma table_info({table});").fetchall()
            table_fk = cur.execute(f"pragma foreign_key_list({table});").fetchall()
            table_index = cur.execute(f"pragma index_list({table});").fetchall()
            unique_columns = []
            for index in table_index:
                if index["unique"] == 1:
                    index_info = cur.execute(
                        f"pragma index_info({index['name']});"
                    ).fetchone()
                    unique_col = str(index_info["name"])
                    unique_columns.append(unique_col)

            if len(table_info) > 0:
                columns = []
                for column in table_info:
                    colname = column["name"]
                    if colname == "values":
                        colname = '"values"'
                    nvi_col = NviColumn(
                        name=colname,
                        datatype=column["type"],
                        notnull=bool(column["notnull"]),
                        pk=bool(column["pk"]),
                        unique=column["name"] in unique_columns,
                    )
                    columns.append(nvi_col)
            else:
                columns = []

            if len(table_fk) > 0:
                foreign_keys = []
                for fk in table_fk:
                    nvi_fk = NviForeignKey(
                        column=fk["from"], ref_table=fk["table"], ref_column=fk["to"]
                    )
                    foreign_keys.append(nvi_fk)
            else:
                foreign_keys = []

        return cls(name=table, columns=columns, foreign_keys=foreign_keys)


class NviGPKG:
    """
    Class that reads the definition of NVI 2023 GPKGs.
    Use it to get sql statements needed to recreate the gpkg.

    By adding constraints from nvi_gpkg_constraints.py
    foreign key relationships between tables can be found or other
    constraints.

    usage: NviGPKG.create_from_gpkg_file(gpkg_file_path)
    """

    def __init__(self, file_path: Path, tables: List[NviTable] = []) -> None:
        self.file_path = file_path
        self.tables = tables

    @classmethod
    def create_from_gpkg_file(cls, file_path: Path):
        table_names = NviGPKG.read_table_names_from_gpkg_file(file_path)
        nvi_tables = []
        for table in table_names:
            nvi_table = NviTable.from_gpkg(file_path, table)
            if nvi_table is not None:
                nvi_tables.append(nvi_table)
        return cls(file_path, nvi_tables)

    def table_by_name(self, tablename: str) -> Optional[NviTable]:
        matches = [table for table in self.tables if table.name == tablename]
        if len(matches) > 0:
            return matches[0]
        else:
            return None

    def create_copy(
        self, copy_dir: Optional[Path] = None, new_name: Optional[str] = None
    ):
        """
        creates a copy of the actual geopackage.
        """
        if copy_dir is None:
            copy_dir = self.file_path.parent
        if new_name is None:
            copy_path = copy_dir / f"{self.file_path.stem}_copy.gpkg"
        else:
            if new_name.endswith(".gpkg"):
                copy_path = copy_dir / new_name
            else:
                copy_path = copy_dir / f"{new_name}.gpkg"

        # create copy of geopackage
        try:
            shutil.copy2(self.file_path, copy_path)
            # create NviGPKG object for check gpkg
            return NviGPKG(copy_path, copy.deepcopy(self.tables))
        except Exception as e:
            print(e)
            return None

    def sort_tables_by_fk(self) -> None:
        """sort tables for a valid creation order"""
        # TODO:
        # currently sorts based on number of Foreign Keys.
        # Create a table dependency hierarchy for sort ordering
        self.tables.sort(key=lambda x: len(x.foreign_keys))

    @staticmethod
    def read_table_names_from_gpkg_file(file_path: Path) -> List[str]:
        """read tables from gpkg"""
        if not file_path.is_file():
            raise FileNotFoundError(f"File: {file_path} does not exists")

        if file_path.suffix != ".gpkg":
            raise FileNotFoundError(f"File: {file_path} is not a GeoPackage")

        all_tables = "select name from sqlite_schema where type='table'"
        excluded_tables = ["%rtree%", "gpkg%", "layer_styles", "sqlite_%"]
        sql = all_tables + "".join(
            [f" and name not like '{excl}'" for excl in excluded_tables]
        )
        with closing(sqlite3.Connection(file_path)) as con:
            result = con.execute(sql).fetchall()
        return [i[0] for i in result]
