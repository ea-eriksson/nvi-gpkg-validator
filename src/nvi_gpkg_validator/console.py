import argparse

from nvi_gpkg_validator.validator import NviGPGKValidator

try:
    from osgeo import ogr

    gdal_loaded = True
except:
    gdal_loaded = False


def run():
    parser = argparse.ArgumentParser(
        prog="nvi-gpkg-validator", description="Validate GeoPackages used as the exchange format for the Swedish NVI standard SS 199000:2023."
    )

    parser.add_argument("filename", help="path to NVI GPKG to validate")
    args = parser.parse_args()

    validator = NviGPGKValidator(args.filename)

    validations = {
        "FOREIGN KEY": validator.validate_foreign_key_constraints,
        "NOT NULL": validator.validate_notnull_constraints,
        "CHECK CONSTRAINT": validator.validate_check_constraints,
        "SQLITE INTEGRITY": validator.validate_integrity,
        "DATETIME FORMAT": validator.validate_datetime_formats,
    }
    if gdal_loaded:
        validations["GEOMETRY TYPE"] = validator.validate_geometry_types
        validations["SPATIAL REFERENCE SYSTEMS"] = validator.validate_spatial_ref

    for validation, func in validations.items():
        print(f"\n{validation} VALIDATION RESULT:")
        try:
            errors = func()
            if len(errors) > 0:
                for error in errors:
                    print(error)
            else:
                print("NO VIOLATIONS FOUND")
        except Exception as e:
            print(f"---- {validation} VALIDATION FAILED ----")


if __name__ == "__main__":
    run()
