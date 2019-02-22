export var metaDataQueries = [
        { "key":"columns_total", 
          "label":"list of columns", 
          "query":"SELECT * FROM information_schema.Columns;" 
        },

        { "key":"columns_withkey",
          "label":"column info with keys",
          "query": `SELECT c.table_name, c.column_name, c.DATA_TYPE, c.IS_NULLABLE, 
                        k.constraint_type, k.constraint_name
                    FROM information_schema.columns as c 
                    left join
                    (
                        select col.column_name, tab.table_name, tab.constraint_type, tab.constraint_name
                        FROM   information_schema.constraint_column_usage as col
                        join information_schema.table_constraints as tab
                        on col.constraint_name = tab.constraint_name
                        where tab.table_name = col.table_name
                    )
                    as k
                    on c.column_name = k.column_name
                    and c.table_name = k.table_name;`,

        },

        { "key":"primaries",
          "label":"table key info",
          "query": `SELECT col.column_name, tab.table_name, tab.constraint_type, col.constraint_name
                    FROM   information_schema.constraint_column_usage as col
                    JOIN information_schema.table_constraints as tab
                    ON col.constraint_name = tab.constraint_name
                    WHERE tab.table_name = col.table_name;`
        },

        { "key":"columns_abridged",
          "label":"column info abridged",
          "query": `SELECT table_name, column_name, ordinal_position, data_type, is_nullable
                    FROM information_schema.columns;`,
        },

        { "key":"information_shema.tables",
          "label":"list of tables",
          "query": `SELECT * from information_schema.tables;`,
        }
    ];


export var medicalTableNames = [
    "tblHealthHomePatients",
    "rPatient",
    "Provider",
    "PharmacyClaims",
    "rFacilityClaim",
    "rProfessionalClaim",
    "rCMAttributeItem",
    "rCMAttribute",
    "FacilityClaimDetail",
    "FacilityClaimMoney",
    "FacilityClaimDetailMoney",
    "rServiceClaimDiagnosis",
    ];

/*
  ProviderID may be called PrescriberID or FacilityProviderID, depending on the table. HCHProvID is a ProviderID.
  A ServiceClaimDiagnosis will have either an associated FacilityClaimID or ProfessionalClaimID but not both.
*/

export var medicalTables = [
    { "name" : "tblHealthHomePatients",
      "pkey" : "newID",
      "fkeys": ["HCHProvID"],
    },

    { "name" : "rPatient",
      "pkey" : "newID",
      "fkeys": [],
    },

    { "name" : "Provider",
      "pkey" : "PrividerID",
      "fkeys": [],
    },

    { "name" : "PharmacyClaims",
      "pkey" : "PharmacyClaimID",
      "fkeys": ["newID","PrescriberID"],
    },

    { "name" : "rFacilityClaim",
      "pkey" : "FacilityClaimID",
      "fkeys": ["newID"],
    },

    { "name" : "rProfessionalClaim",
      "pkey" : "ProfessionalClaimID",
      "fkeys": ["ProviderID"],
    },

    { "name" : "rCMAttributeItem",
      "pkey" : "CMAttributeItemID",
      "fkeys": ["CMAttributeID","newID"],
    },

    { "name" : "rCMAttribute",
      "pkey" : "CMAttributeID",
      "fkeys": [],
    },

    { "name" : "FacilityClaimDetail",
      "pkey" : "FacilityClaimDetailID",
      "fkeys": ["FacilityClaimID"],
    },

    { "name" : "FacilityClaimMoney",
      "pkey" : null,
      "fkeys": [],
    },

    { "name" : "FacilityClaimDetailMoney"
      "pkey" : null,
      "fkeys": [],
    },

    { "name" : "rServiceClaimDiagnosis",
      "pkey" : "ServiceClaimDiagnosisID",
      "fkeys": ["FacilityClaimID","ProfessionalClaimID","newID"],
    },
];
