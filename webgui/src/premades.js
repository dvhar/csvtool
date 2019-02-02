export var metaDataQueries = [
        { "key":"columns_total", 
          "label":"informationSchema.columns", 
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
        }
    ]
