from nvi_gpkg_validator.nvi_gpkg import NviGPKG


class NviGPKGForeignKeyConstraints:
    """
    Defines not null columns in NVI 2023 GPKG.
    Note: it only defines it in the NviGPKG class
    which contains definitions of the gpkg (create table statements, etc..)
    and does not add the constraints to the actual gpkg.

    usage: NviGPKGForeignKeyConstraints().add_to_gpkg(NviGPKG)
    """

    def __init__(self):
        # dict with foreign keys to vl_tables
        self.vl_fk = {  # column : vl_table
            "kartl_typAvKartlaggning": "VL_Kartlaggningstyp",
            "kartl_minstaKarteringsenh": "VL_Karteringsenhet",
            "typAvBiotopsskyddsomr": "VL_Biotypskyddomradestyp",
            "kvantifiering": "VL_KvantifieringArtforekomster",
            "livsmiljonsLamplighet": "VL_LivsmiljosGradAvLamplighet",
            "vardeelementtyp": "VL_Vardeelementtyp",
            "tradstatus": "VL_Tradstatus",
            "tradvitalitet": "VL_Tradvitalitet",
            "hydromorfologiskTyp": "VL_HydromorfologiskTyp",
            "hydromorfologiskTypkod": "VL_HydromorfologiskTypkod",
            "naturtyp": "VL_NaturtypNVI",
            "naturvardesklass": "VL_Naturvardesklass",
            "ovrigVardeklass": "VL_VardesklassOvrigaBiotoper",
            "BiotopbeteckningNVI": "VL_BiotopbeteckningNVI",
            "N2000naturtypskod": "VL_Natura2000Naturtypskoder",
            "N2000naturtypsnamn": "VL_Natura2000Naturtypsnamn",
            "SarskSkyddsvTradKriterier": "VL_SarskSkyddsvTradKriterier",
            "motivering": "VL_KanneteckenNaturvardestrad",
            "livsmiljoFunktion": "VL_LivsmiljoFunktion",
        }

    def add_to_gpkg(self, gpkg: NviGPKG) -> None:
        """Add foreign keys to NviGPKG definition"""
        # set referenced columns unique
        for table in gpkg.tables:
            for column in table.columns:
                if column.name in ["objektidentitet", '"values"']:
                    column.unique = True

        # add foreign keys
        for table in gpkg.tables:
            for column in table.columns:
                ref_column = None
                ref_table = None
                if column.name.startswith("FK_"):
                    ref_column = "objektidentitet"
                    ref_table = column.name.split("FK_")[1]
                    if ref_table == "KartlaggningBM":
                        ref_table = "KartlaggningBiologiskMangfald"
                elif column.name in self.vl_fk.keys():
                    ref_column = '"values"'
                    ref_table = self.vl_fk[column.name]
                if ref_column is not None and ref_table is not None:
                    table.add_foreign_key(column.name, ref_table, ref_column)
        # sort tables for correct table creation order
        gpkg.sort_tables_by_fk()


class NviGPKGCheckConstraints:
    """
    Defines not null columns in NVI 2023 GPKG.
    Note: it only defines it in the NviGPKG class
    which contains definitions of the gpkg (create table statements, etc..)
    and does not add the constraints to the actual gpkg.

    usage: NviGPKGCheckConstraints().add_to_gpkg(NviGPKG)
    """

    def __init__(self):
        self.check_constraints = {  # constraint description: [constraint, [columns it applies to]]
            "kvantifiering": {
                "check": "kvantifiering is not null or altTillKvantifiering is not null",
                "columns": ["kvantifiering", "altTillKvantifiering"],
            },
            "taxon nvi": {
                "check": "taxon_svensktNamn is not null or taxon_vetenskapligtNamn is not null",
                "columns": ["taxon_svensktNamn", "taxon_vetenskapligtNamn"],
            },
            "motivering värdelandskap": {
                "check": "vardelandskap in (False, 'Nej', 'nej') or motiveringVardelandskap is not Null",
                "columns": ["vardelandskap", "motiveringVardelandskap"],
            },
            "forklaring preliminär värdeklass": {
                "check": "preliminarVardesklass in (False, 'Nej', 'nej') or forkTillPrelVardesklass is not Null",
                "columns": ["preliminarVardesklass", "forkTillPrelVardesklass"],
            },
            "forklaring preliminär avgränsning": {
                "check": "preliminarAvgransning in (False, 'Nej', 'nej') or forkTillPrelAvgransning is not Null",
                "columns": ["preliminarAvgransning", "forkTillPrelVardesklass"],
            },
            "vattendrag hydrotyp": {
                "check": "naturtyp not like 'vattendrag' or (hydromorfologiskTyp is not Null and hydromorfologiskTypkod is not null)",
                "columns": [
                    "naturtyp",
                    "hydromorfologiskTyp",
                    "hydromorfologiskTypkod",
                ],
            },
            "naturvärdesklass eller ovrigVardeklass": {
                "check": "naturvardesklass is not NULL or ovrigVardeklass is not NULL",
                "columns": ["naturvardesklass", "ovrigVardeklass"],
            },
            "livsmiljö lämplighet eller alternativ till livsmiljö lämplighet": {
                "check": "livsmiljonsLamplighet is not NULL or altTillLivsmiljLamplighet is not NULL",
                "columns": ["livsmiljonsLamplighet", "altTillLivsmiljLamplighet"],
            },
            "minsta karteringsenhet": {
                "check": "kartl_typAvKartlaggning not like 'NVI%' and kartl_typAvKartlaggning not like 'fördjupad inventering - Övriga biotoper' OR kartl_minstaKarteringsenh is not Null",
                "columns": ["kartl_minstaKarteringsenh", "kartl_typAvKartlaggning"],
            },
            "FK_Livsmiljo": {
                "check": "FK_Livsmiljo_punkt is not null or FK_Livsmiljo_yta is not null",
                "columns": ["FK_Livsmiljo_punkt", "FK_Livsmiljo_yta"],
                "tables": ["livsmiljonsBedomdaFunktioner"],
            },
            "FK_SarskSkyddsvTrad": {
                "check": "FK_SarskSkyddsvTrad_punkt is not null or FK_SarskSkyddsvTrad_yta is not null or FK_Naturvardestrad_punkt is not null or FK_Naturvardestrad_yta is not null",
                "columns": [
                    "FK_SarskSkyddsvTrad_punkt",
                    "FK_SarskSkyddsvTrad_yta",
                    "FK_Naturvardestrad_punkt",
                    "FK_Naturvardestrad_yta",
                ],
                "tables": ["SarskiltSkyddsvartTrad"],
            },
            "FK_Naturvardestrad": {
                "check": "FK_Naturvardestrad_punkt is not null or FK_Naturvardestrad_yta is not null",
                "columns": ["FK_Naturvardestrad_punkt", "FK_Naturvardestrad_yta"],
                "tables": ["motivering"],
            }
        }

    def add_to_gpkg(self, gpkg: NviGPKG) -> None:
        """add check constraints to NviGPKG definition"""
        for table in gpkg.tables:
            for check_name, check_dict in self.check_constraints.items():
                if set(check_dict["columns"]).issubset(set(table.list_column_names())):
                    if "tables" not in check_dict.keys():
                        table.add_check_constraint(check_dict["check"])
                    else:
                        if table.name in check_dict["tables"]:
                            table.add_check_constraint(check_dict["check"])


class NviGPKGNotNullConstraints:
    """
    Defines not null columns in NVI 2023 GPKG.
    Note: it only defines it in the NviGPKG class
    which contains definitions of the gpkg (create table statements, etc..)
    and does not add the constraints to the actual gpkg.

    usage: NviGPKGNotNullConstraints().add_to_gpkg(NviGPKG)
    """

    def __init__(self):
        self.notnull_all_tables = [
            "BiotopbeteckningNVI",
            "FK_KartlaggningBM",
            "anledning",
            "artvarden",
            "beskrAvKartlaggomr",
            "bestOrg_orgNamn",
            "bestOrg_orgNummer",
            "biotopvarden",
            "datum",
            "datumForFaltbesok",
            "datumForObjektavgr",
            "kalla",
            "kartl_typAvKartlaggning",
            "vardeelementtyp",
            "vardelandskap",
            "versionGiltigFran",
            "stamomkrets",
            "tidsperiod_fran",
            "tidsperiod_til",
            "tradstatus",
            "typAvBiotopsskyddsomr",
            "utfOrg_orgNamn",
            "utfOrg_orgNummer",
            "utforare",
            "preliminarVardesklass",
            "projektidentitet",
            "objektversion",
            "objektidentitet",
        ]
        self.notnull_specific_tables = [
            {"column": "fortsatterUtanforInvomr", "tables": ["NVINaturvardesbiotop"]},
            {"column": "hydromorfologiskTyp", "tables": ["VattendragDelstracka"]},
            {"column": "hydromorfologiskTypkod", "tables": ["VattendragDelstracka"]},
            {"column": "id", "tables": ["ReferensTillUnderlag"]},
            {"column": "invasivaFrammandeArter", "tables": ["NVINaturvardesbiotop"]},
            {"column": "naturtyp", "tables": ["NVINaturvardesbiotop", "OvrigBiotop"]},
            {"column": "naturvardesklass", "tables": ["NVINaturvardesbiotop"]},
            {
                "column": "objektbeskrivning",
                "tables": [
                    "NVILandskapsomrade",
                    "NVINaturvardesbiotop",
                    "Livsmiljo_yta",
                    "Livsmiljo_punkt",
                ],
            },
            {
                "column": "objektnummer",
                "tables": [
                    "NVILandskapsomrade",
                    "NVINaturvardesbiotop",
                    "Vardeelement_yta",
                ],
            },
            {"column": "ovrigVardeklass", "tables": ["OvrigBiotop"]},
            {
                "column": "preliminarAvgransning",
                "tables": ["NVINaturvardesbiotop", "VattendragDelstracka"],
            },
            {"column": "referenser", "tables": ["NVINaturvardesbiotop"]},
            {"column": "vardearterKandaTidigare", "tables": ["NVINaturvardesbiotop"]},
            {
                "column": "vardearterObserverade",
                "tables": [
                    "NVINaturvardesbiotop",
                    "VattendragDelstracka",
                    "Smavatten_yta",
                    "Smavatten_punkt",
                    "Bottenmiljo_yta",
                    "Bottenmiljo_punkt",
                ],
            },
        ]

    def add_to_gpkg(self, gpkg: NviGPKG, as_check_constraint=False) -> None:
        """add not null constraints to NviGPKG definition"""
        for notnull in self.notnull_specific_tables:
            for notnulltable in notnull["tables"]:
                table = gpkg.table_by_name(notnulltable)
                if table is not None:
                    if as_check_constraint:
                        table.add_check_constraint(f"{notnull['column']} is not null")
                    else:
                        table.set_column_notnull(notnull["column"])
        for notnull in self.notnull_all_tables:
            for table in gpkg.tables:
                if notnull in table.list_column_names():
                    if as_check_constraint:
                        table.add_check_constraint(f"{notnull} is not null")
                    else:
                        table.set_column_notnull(notnull)
