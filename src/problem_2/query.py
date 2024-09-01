import psycopg2
from psycopg2 import sql
import polars as pl
from datetime import datetime
from typing import List, Tuple, Union


def query_data(
    conn, filter_arguments: List[List[Union[str, List[str]]]], output_columns: List[str]
) -> pl.DataFrame:
    cur = conn.cursor()

    # Base query with placeholders for columns and joins
    query = sql.SQL("""
        WITH event_data AS (
            SELECT event_url,
                MAX(CASE WHEN attribute = 'event_name' THEN value END) AS event_name,
                MAX(CASE WHEN attribute = 'event_start_date' THEN value::date END) AS event_start_date,
                MAX(CASE WHEN attribute = 'event_city' THEN value END) AS event_city,
                MAX(CASE WHEN attribute = 'event_country' THEN value END) AS event_country
            FROM event_attributes
            GROUP BY event_url
        ),
        company_data AS (
            SELECT company_url,
                MAX(CASE WHEN attribute = 'company_name' THEN value END) AS company_name,
                MAX(CASE WHEN attribute = 'company_country' THEN value END) AS company_country,
                MAX(CASE WHEN attribute = 'company_industry' THEN value END) AS company_industry,
                MAX(CASE WHEN attribute = 'company_event_url' THEN value END) AS company_event_url
            FROM company_attributes
            GROUP BY company_url
        ),
        person_data AS (
            SELECT person_id,
                MAX(CASE WHEN attribute = 'person_first_name' THEN value END) AS person_first_name,
                MAX(CASE WHEN attribute = 'person_last_name' THEN value END) AS person_last_name,
                MAX(CASE WHEN attribute = 'person_email' THEN value END) AS person_email,
                MAX(CASE WHEN attribute = 'person_seniority' THEN value END) AS person_seniority,
                MAX(CASE WHEN attribute = 'person_department' THEN value END) AS person_department,
                MAX(CASE WHEN attribute = 'person_company_url' THEN value END) AS person_company_url
            FROM people_attributes
            GROUP BY person_id
        )
        SELECT {columns}
        FROM event_data e
        LEFT JOIN company_data c ON e.event_url = c.company_event_url
        LEFT JOIN person_data p ON c.company_url = p.person_company_url
        WHERE 1=1
        {conditions}
    """)

    def add_alias(col):
        if col.startswith("event_"):
            return sql.SQL("e.{}").format(sql.Identifier(col))
        elif col.startswith("company_"):
            return sql.SQL("c.{}").format(sql.Identifier(col))
        elif col.startswith("person_"):
            return sql.SQL("p.{}").format(sql.Identifier(col))
        else:
            return sql.Identifier(col)

    columns = sql.SQL(", ").join(map(add_alias, output_columns))

    conditions = []
    params = []
    for col, op, val in filter_arguments:
        table_alias = (
            "e"
            if col.startswith("event_")
            else "c"
            if col.startswith("company_")
            else "p"
        )

        if op == "includes":
            if isinstance(val, list):
                if len(val) == 1:
                    conditions.append(
                        sql.SQL("AND {}.{} = %s").format(
                            sql.Identifier(table_alias), sql.Identifier(col)
                        )
                    )
                    params.append(val[0])
                else:
                    conditions.append(
                        sql.SQL("AND {}.{} = ANY(%s)").format(
                            sql.Identifier(table_alias), sql.Identifier(col)
                        )
                    )
                    params.append(val)
            else:
                conditions.append(
                    sql.SQL("AND {}.{} = %s").format(
                        sql.Identifier(table_alias), sql.Identifier(col)
                    )
                )
                params.append(val)
        elif op in ["greater-than-equal-to", "less-than-equal-to"]:
            if col == "event_start_date":
                try:
                    date_val = datetime.strptime(val, "%Y-%m-%d").date()
                except ValueError:
                    raise ValueError(
                        f"Invalid date format for {col}: {val}. Expected format: YYYY-MM-DD"
                    )
                op_symbol = ">=" if op == "greater-than-equal-to" else "<="
                conditions.append(
                    sql.SQL(f"AND {{}}::date {op_symbol} %s").format(
                        sql.Identifier(table_alias, col)
                    )
                )
                params.append(date_val)
            else:
                op_symbol = ">=" if op == "greater-than-equal-to" else "<="
                conditions.append(
                    sql.SQL(f"AND {{}} {op_symbol} %s").format(
                        sql.Identifier(table_alias, col)
                    )
                )
                params.append(val)
        else:
            raise ValueError(f"Unsupported operation: {op}")

    conditions_sql = sql.SQL(" ").join(conditions)

    final_query = query.format(columns=columns, conditions=conditions_sql)

    try:
        cur.execute(final_query, params)
        results = cur.fetchall()
        return pl.DataFrame(results, schema=output_columns)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return pl.DataFrame(schema=output_columns)
    finally:
        cur.close()
