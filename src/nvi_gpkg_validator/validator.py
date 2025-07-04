import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

from nvi_gpkg_validator.constraints import (
    NviGPKGCheckConstraints,
    NviGPKGForeignKeyConstraints,
    NviGPKGNotNullConstraints,
)
from nvi_gpkg_validator.nvi_gpkg import NviGPKG

try:
    from osgeo import ogr

    gdal_loaded = True
except:
    gdal_loaded = False


@dataclass
class NviViolation:
    table: str
    row: Optional[int]


@dataclass
class NviSpatialRefViolation:
    spat_refs: Union[set, list]

    def __str__(self):
        return f"SPATIAL REF VIOLATION - multiple crs used in tables in gpkg: {self.spat_refs}"


@dataclass
class NviGeometryTypeViolation(NviViolation):
    defined_type: str
    wrong_type: str

    def __str__(self):
        return f"GEOMETRY TYPE VIOLATION - table: {self.table} ({self.defined_type}) has rows of {self.wrong_type} geometry type"


@dataclass
class NviDateTimeFormatViolation(NviViolation):
    column: str

    def __str__(self):
        return f"DATETIME FORMAT VIOLATION - table: {self.table} column: {self.column} - incorrect DateTime format"


@dataclass
class NviIntegrityViolation:
    violation: str

    def __str__(self):
        return f"SQLITE INTEGRITY CHECK: {self.violation}"


@dataclass
class NviFKViolation(NviViolation):
    column: str
    errorvalue: str
    ref_table: str
    ref_column: str

    def __str__(self) -> str:
        return (
            f"FOREIGN KEY VIOLATION - row: {self.row} "
            f"value: {self.errorvalue} "
            f"in {self.table}({self.column}) "
            f"not in {self.ref_table}({self.ref_column})"
        )


@dataclass
class NviNotNullViolation(NviViolation):
    columns: list

    def __str__(self) -> str:
        return f"NOT NULL WARNING: table: {self.table} row: {self.row} - NULL value in {self.columns}"


@dataclass
class NviCheckViolation(NviViolation):
    check: str

    def __str__(self) -> str:
        return_str = f"CHECK CONSTRANT WARNING - table: {self.table} "
        if self.row is not None:
            return_str += f"row: {self.row}"
        return_str += f": {self.check}"
        return return_str


class NviGPGKValidator:
    """
    class used to validate whether NVI 2023 GeoPackages
    conform to the standard.

    The tests performed are not complete and errors in
    non-conformant gpkgs may not be found.

    Tests performed are:
        - foreign key checks
        - not null columns
        - check constraints
        - datetime/date columns format according to gpkg standard
        - correct geometry type in geom column
        - all tables have the same spatial ref system

    Note:
    Foreign key errors are only noticed if the referencing column is not null.
    - else it may be found in the not null checks.

    not null columns depends on the delivery type, and this only provides a
    general test, not applicable to all delivery types.
    """

    def __init__(self, gpkg_path: Union[Path, str]) -> None:
        self.gpkg = NviGPKG.create_from_gpkg_file(Path(gpkg_path))

    def _get_validation_gpkg(self, validationtype: str = "_validation") -> NviGPKG:
        return self.gpkg.create_copy(new_name=self.gpkg.file_path.stem + validationtype)

    def validate_datetime_formats(self) -> List[NviDateTimeFormatViolation]:
        errors = []
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="dateformat"
            )
            # check datetime columns
            with closing(sqlite3.Connection(gpkg_copy.file_path)) as con:
                with con:
                    for table in gpkg_copy.tables:
                        dt_cols = con.execute(
                            f"select name, type from pragma_table_info('{table.name}') where type in ('DATETIME', 'DATE')"
                        ).fetchall()
                        for row in dt_cols:
                            colname = row[0]
                            dt_type = row[1]
                            if dt_type == "DATE":
                                strftime_pattern = "%F"
                            else:
                                strftime_pattern = "%FT%R:%fZ"
                            sql = f"""select 1 from {table.name}
                                    where {colname} not like strftime('{strftime_pattern}', {colname})
                                """
                            if dt_type != "DATE":
                                sql += f" or {colname} not like '%Z'" 
                            result = con.execute(sql).fetchall()
                            if len(result) > 0:
                                errors.append(
                                    NviDateTimeFormatViolation(
                                        table=table.name, row=None, column=colname
                                    )
                                )

            gpkg_copy.file_path.unlink()
        return errors

    def validate_geometry_types(self) -> List[NviGeometryTypeViolation]:
        """check that all features in table have the defined geometry type"""
        if not gdal_loaded:
            raise ImportError(
                "GDAL/OGR module not imported - geometry type validation not available"
            )
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="geomtype"
            )
            errors = []
            with ogr.Open(gpkg_copy.file_path) as con:
                for i in range(con.GetLayerCount()):
                    lyr = con.GetLayer(i)
                    geom_col = lyr.GetGeometryColumn()
                    if geom_col:
                        def_geom_type = lyr.GetGeomType()
                        def_geom_type_name = ogr.GeometryTypeToName(def_geom_type)
                        # check if geometry types found in layer
                        # corresponds to defined type
                        geom_types = lyr.GetGeometryTypes()
                        for geom_type, n_feats in geom_types.items():
                            if geom_type != def_geom_type:
                                geom_type_name = ogr.GeometryTypeToName(geom_type)
                                errors.append(
                                    NviGeometryTypeViolation(
                                        table=lyr.GetName(),
                                        row=None,
                                        defined_type=def_geom_type_name,
                                        wrong_type=geom_type_name,
                                    )
                                )
            con = None
            lyr = None
            gpkg_copy.file_path.unlink()
        return errors

    def validate_spatial_ref(self) -> List[NviSpatialRefViolation]:
        """check that all tables have the same spatial ref defined"""
        if not gdal_loaded:
            raise ImportError(
                "GDAL/OGR module not imported - spatial ref validation not available"
            )

        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="spatref"
            )
            spat_refs = set()
            with ogr.Open(gpkg_copy.file_path) as con:
                for i in range(con.GetLayerCount()):
                    spat_ref = con.GetLayer(i).GetSpatialRef()
                    if spat_ref:
                        spat_refs.add(spat_ref.GetName())
            spat_ref = None
            con = None
            gpkg_copy.file_path.unlink()

        errors = []
        if len(spat_refs) > 1:
            errors.append(NviSpatialRefViolation(spat_refs))
        return errors

    def validate_foreign_key_constraints(self) -> List[NviFKViolation]:
        """validate foreign keys defined in
        nvi_gpkg_constraints.NviGPKGForeignKeyConstraints()
        """
        errors = []
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="foreign-keys"
            )
            NviGPKGForeignKeyConstraints().add_to_gpkg(gpkg_copy)
            # perform validation sql
            with closing(sqlite3.Connection(gpkg_copy.file_path)) as con:
                with con:
                    con.row_factory = sqlite3.Row
                    try:
                        con.execute("PRAGMA foreign_keys = OFF;")
                        for table in gpkg_copy.tables:
                            con.execute(
                                f"ALTER TABLE {table.name} rename to {table.name}_old"
                            )
                            con.execute(table.sql())
                            con.execute(
                                f"INSERT INTO {table.name} SELECT * FROM {table.name}_old"
                            )
                            con.execute(f"DROP TABLE {table.name}_old")
                        con.execute("PRAGMA foreign_keys = ON;")
                        for table in gpkg_copy.tables:
                            if len(table.foreign_keys) > 0:
                                fk_test = con.execute(
                                    f"PRAGMA foreign_key_check({table.name});"
                                ).fetchall()
                                if len(fk_test) > 0:
                                    fk_list = con.execute(
                                        f"PRAGMA foreign_key_list({table.name})"
                                    ).fetchall()
                                    for error in fk_test:
                                        rowid = error["rowid"]
                                        ref_table = error["parent"]
                                        fk_id = error["fkid"]
                                        fk_info = [
                                            i for i in fk_list if i["id"] == fk_id
                                        ][0]
                                        from_col = fk_info["from"]
                                        to_col = fk_info["to"]
                                        fk_value = con.execute(
                                            f"select {from_col} from {table.name} where id={rowid}"
                                        ).fetchone()[0]
                                        errors.append(
                                            NviFKViolation(
                                                table=table.name,
                                                row=int(rowid),
                                                column=from_col,
                                                errorvalue=str(fk_value),
                                                ref_table=ref_table,
                                                ref_column=to_col,
                                            )
                                        )
                        con.rollback()
                    except Exception as e:
                        con.rollback()
                        print(f"Exception: {e}")

            # remove gpkg_copy
            gpkg_copy.file_path.unlink()
        return errors

    def validate_notnull_constraints(self):
        """validate notnull columns defined in
        nvi_gpkg_constraints.NviGPKGNotNullConstraints()
        """
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="not-null"
            )
            NviGPKGNotNullConstraints().add_to_gpkg(gpkg_copy)
            # perform validation sql
            errors = []
            with closing(sqlite3.Connection(gpkg_copy.file_path)) as con:
                with con:
                    con.row_factory = sqlite3.Row
                    for table in gpkg_copy.tables:
                        if table.name.startswith("Vl_"):
                            continue
                        notnull_cols = table.list_notnull_columns()
                        if len(notnull_cols) > 0:
                            where = " IS NULL OR ".join(notnull_cols)
                            test_sql = (
                                f"SELECT * FROM {table.name} WHERE {where} IS NULL"
                            )
                            result = con.execute(test_sql).fetchall()
                            for row in result:
                                row_notnull_violations = []
                                for col in notnull_cols:
                                    col_value = row[col]
                                    if col_value is None:
                                        row_notnull_violations.append(col)
                                error = NviNotNullViolation(
                                    table.name, row["id"], row_notnull_violations
                                )
                                errors.append(error)

            # remove gpkg_copy
            gpkg_copy.file_path.unlink()
        return errors

    def validate_check_constraints(self, row_violations: bool = True):
        """validate check constraints defined in
        nvi_gpkg_constraints.NviGPKGCheckConstraints()
        """
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="check-constraints"
            )
            NviGPKGCheckConstraints().add_to_gpkg(gpkg_copy)
            # validate by turning off check constraints
            errors = []
            with closing(sqlite3.Connection(gpkg_copy.file_path)) as con:
                with con:
                    con.row_factory = sqlite3.Row
                    try:
                        con.execute("PRAGMA foreign_keys = OFF;")
                        for table in gpkg_copy.tables:
                            if len(table.check_constraints) == 0:
                                continue
                            con.execute("PRAGMA ignore_check_constraints = ON;")
                            con.execute(
                                f"ALTER TABLE {table.name} rename to {table.name}_old"
                            )
                            con.execute(table.sql())
                            if row_violations:
                                table_data = con.execute(
                                    f"SELECT * FROM {table.name}_old"
                                ).fetchall()
                                con.execute("PRAGMA ignore_check_constraints = OFF;")
                                for row in table_data:
                                    try:
                                        insert = f"INSERT INTO {table.name} VALUES ({', '.join('?' * len(row))})"
                                        con.execute(insert, row)
                                    except sqlite3.Error as e:
                                        error = NviCheckViolation(
                                            table.name, row["id"], str(e)
                                        )
                                        errors.append(error)
                            else:
                                con.execute(
                                    f"INSERT INTO {table.name} SELECT * FROM {table.name}_old"
                                )
                                con.execute(f"DROP TABLE {table.name}_old")
                                con.execute("PRAGMA ignore_check_constraints = OFF;")
                                result = con.execute(
                                    f"PRAGMA quick_check({table.name});"
                                ).fetchall()
                                for row in result:
                                    for error in row:
                                        if error != "ok":
                                            errors.append(
                                                NviCheckViolation(
                                                    table=table.name,
                                                    row=None,
                                                    check=str(error),
                                                )
                                            )
                        con.rollback()
                    except Exception as e:
                        con.rollback()
                        print(f"Exception: {e}")
            # remove gpkg_copy
            gpkg_copy.file_path.unlink()
        return errors

    def validate_integrity(self) -> List[NviIntegrityViolation]:
        """
        run sqlite integrity check
        """
        with tempfile.TemporaryDirectory() as tempdir:
            gpkg_copy = self.gpkg.create_copy(
                copy_dir=Path(tempdir), new_name="sqlite-integrity"
            )
            # validate by turning off check constraints
            errors = []
            with closing(sqlite3.Connection(gpkg_copy.file_path)) as con:
                with con:
                    con.row_factory = sqlite3.Row
                    try:
                        integrity_check = con.execute(
                            "PRAGMA integrity_check;"
                        ).fetchall()
                        for row in integrity_check:
                            result = row["integrity_check"]
                            errors.append(NviIntegrityViolation(result))
                    except Exception as e:
                        print(f"Exception: {e}")
            # remove gpkg_copy
            gpkg_copy.file_path.unlink()
        return errors
