from importlib.resources import as_file, files
from pathlib import Path
from zipfile import ZipFile

import pytest

from nvi_gpkg_validator.validator import NviGPGKValidator

from . import testgpkg


@pytest.fixture
def nvi_validator(tmp_path):
    datafilepath = files(testgpkg).joinpath("testdata.gpkg.zip")
    with as_file(datafilepath) as testdata_zip:
        with ZipFile(testdata_zip) as zipdata:
            zipdata.extractall(tmp_path)
        yield NviGPGKValidator(Path(tmp_path) / "testdata.gpkg")

def test_foreign_key_validation(nvi_validator):
    errors = nvi_validator.validate_foreign_key_constraints()
    assert len(errors) == 1
    assert_errors = [f"{e.table}-{e.row}" for e in errors]
    assert "informationOmKartlaggning-3" in assert_errors

def test_not_null_validation(nvi_validator):
    errors = nvi_validator.validate_notnull_constraints()
    assert len(errors) == 1
    assert_errors = [f"{e.table}-{e.row}" for e in errors]
    assert "Artforekomst_punkt-3" in assert_errors

def test_check_constraint_validation(nvi_validator):
    errors = nvi_validator.validate_check_constraints()
    assert len(errors) == 1
    assert_errors = [f"{e.table}-{e.row}" for e in errors]
    assert "Artforekomst_punkt-3" in assert_errors

def test_sqlite_integrity_validation(nvi_validator):
    errors = nvi_validator.validate_integrity()
    assert len(errors) == 1
    assert errors[0].violation == 'ok'

def test_datetime_validation(nvi_validator):
    errors = nvi_validator.validate_datetime_formats()
    assert len(errors) == 1
    assert_errors = [f"{e.table}-{e.column}" for e in errors]
    assert "datumForFaltbesok-datumForFaltbesok" in assert_errors

