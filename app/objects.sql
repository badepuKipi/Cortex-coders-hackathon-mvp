create or replace TABLE ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY (
	FIX_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	SUGGESTION_ID NUMBER(38,0),
	LOG_ID NUMBER(38,0),
	DAG_ID VARCHAR(500),
	TASK_ID VARCHAR(500),
	FIX_SQL VARCHAR(16777216),
	FIX_STATUS VARCHAR(50),
	ERROR_MESSAGE VARCHAR(16777216),
	CONFIDENCE_SCORE VARCHAR(50),
	SEVERITY VARCHAR(50),
	FIX_MODE VARCHAR(20) DEFAULT 'AUTO',
	APPLIED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	primary key (FIX_ID)
);


create or replace TABLE ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS (
	SETTING_KEY VARCHAR(100) NOT NULL,
	SETTING_VALUE VARCHAR(500),
	UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	UPDATED_BY VARCHAR(200) DEFAULT CURRENT_USER(),
	primary key (SETTING_KEY)
);


create or replace TABLE ETL_BOT.ETL_SCHEMA.EMPLOYEE (
	EMP_ID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(100),
	SALARY NUMBER(38,0)
);


create or replace TABLE ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS (
	LOG_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	DAG_ID VARCHAR(500) NOT NULL,
	TASK_ID VARCHAR(500) NOT NULL,
	EXECUTION_DATE TIMESTAMP_NTZ(9) NOT NULL,
	FAILURE_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ERROR_MESSAGE VARCHAR(16777216),
	ERROR_TYPE VARCHAR(200),
	LOG_URL VARCHAR(2000),
	TRY_NUMBER NUMBER(38,0) DEFAULT 1,
	AIRFLOW_INSTANCE VARCHAR(500),
	PIPELINE_CONTEXT VARCHAR(16777216),
	IS_ANALYZED BOOLEAN DEFAULT FALSE,
	CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	primary key (LOG_ID)
);


create or replace TABLE ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS (
	SUGGESTION_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	LOG_ID NUMBER(38,0) NOT NULL,
	ROOT_CAUSE VARCHAR(16777216),
	SUGGESTED_FIX VARCHAR(16777216),
	FIX_SQL VARCHAR(16777216),
	SEVERITY VARCHAR(50),
	CONFIDENCE_SCORE NUMBER(5,2),
	CATEGORY VARCHAR(200),
	AUTO_RETRY_RECOMMENDED BOOLEAN DEFAULT FALSE,
	MODEL_USED VARCHAR(200),
	ANALYZED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	primary key (SUGGESTION_ID)
);

create or replace TABLE ETL_BOT.ETL_SCHEMA.SALES_FACT (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_NAME VARCHAR(200) NOT NULL,
	PRODUCT VARCHAR(200) NOT NULL,
	QUANTITY NUMBER(38,0) NOT NULL,
	TOTAL_REVENUE NUMBER(12,2) NOT NULL,
	ORDER_DATE DATE NOT NULL,
	REGION VARCHAR(50) NOT NULL
);

create or replace TABLE ETL_BOT.ETL_SCHEMA.SALES_RAW (
	ORDER_ID NUMBER(38,0),
	CUSTOMER_NAME VARCHAR(200),
	PRODUCT VARCHAR(200),
	QUANTITY NUMBER(38,0),
	PRICE NUMBER(10,2),
	ORDER_DATE DATE,
	REGION VARCHAR(50)
);


create or replace stream ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS_STREAM on table ETL_FAILURE_LOGS append_only = true;


create or replace task ETL_BOT.ETL_SCHEMA.ANALYZE_FAILURES_TASK
	warehouse=ETL_BOT_WH
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS_STREAM')
	as CALL ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES();


create or replace task ETL_BOT.ETL_SCHEMA.AUTO_FIX_TASK
	warehouse=ETL_BOT_WH
	after ETL_BOT.ETL_SCHEMA.ANALYZE_FAILURES_TASK
	as CALL ETL_BOT.ETL_SCHEMA.AUTO_APPLY_FIXES_V2();


create or replace task ETL_BOT.ETL_SCHEMA.INGEST_QUERY_HISTORY_TASK
	warehouse=ETL_BOT_WH
	schedule='5 MINUTE'
	as CALL ETL_BOT.ETL_SCHEMA.INGEST_FROM_QUERY_HISTORY();




CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.ANALYZE_ETL_FAILURE_V2("P_LOG_ID" NUMBER(38,0))
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_error_message STRING;
    v_dag_id STRING;
    v_task_id STRING;
    v_error_type STRING;
    v_pipeline_context STRING;
    v_schema_context STRING;
    v_object_context STRING;
    v_query_history_context STRING;
    v_prompt STRING;
    v_response STRING;
    v_clean STRING;
BEGIN
    SELECT ERROR_MESSAGE, DAG_ID, TASK_ID,
           COALESCE(ERROR_TYPE, ''UNKNOWN''),
           COALESCE(PIPELINE_CONTEXT, '''')
    INTO v_error_message, v_dag_id, v_task_id, v_error_type, v_pipeline_context
    FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS
    WHERE LOG_ID = :P_LOG_ID;

    SHOW COLUMNS IN SCHEMA ETL_BOT.ETL_SCHEMA;
    SELECT LISTAGG(
        "table_name" || ''.'' || "column_name" || '' ('' || PARSE_JSON("data_type"):type::VARCHAR ||
        CASE WHEN PARSE_JSON("data_type"):nullable::BOOLEAN = FALSE THEN '' NOT NULL'' ELSE '' NULLABLE'' END || '')'', ''\\n''
    ) INTO v_schema_context
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW TABLES IN SCHEMA ETL_BOT.ETL_SCHEMA;
    SELECT LISTAGG(''TABLE: ETL_BOT.ETL_SCHEMA.'' || "name" || '' | ROWS: '' || "rows", ''\\n'') INTO v_object_context
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    v_query_history_context := '''';
    IF (v_pipeline_context IS NOT NULL AND v_pipeline_context != '''') THEN
        BEGIN
            LET v_query_id STRING := '''';
            SELECT TRY_PARSE_JSON(:v_pipeline_context):query_text::VARCHAR INTO v_query_history_context;
            IF (v_query_history_context IS NOT NULL) THEN
                v_query_history_context := ''Original failing query: '' || v_query_history_context;
            ELSE
                v_query_history_context := '''';
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_query_history_context := '''';
        END;
    END IF;

    v_prompt := ''
You are a Snowflake ETL failure auto-fix bot. Analyze this error and generate ONE correct fix.

ERROR DETAILS:
- DAG: '' || v_dag_id || ''
- Task (Airflow task_id, NOT the Snowflake procedure name): '' || v_task_id || ''
- Error Type: '' || v_error_type || ''
- Error Message: '' || v_error_message || ''
'' || CASE WHEN v_query_history_context != '''' THEN ''
FAILING SQL FROM QUERY HISTORY:
'' || v_query_history_context ELSE '''' END || ''

EXISTING SCHEMA OBJECTS:
'' || COALESCE(v_object_context, ''N/A'') || ''

KNOWN PROCEDURES IN THIS SCHEMA: LOAD_SALES_FACT() - loads data from SALES_RAW into SALES_FACT.

SCHEMA COLUMNS (table.column type nullable):
'' || COALESCE(LEFT(v_schema_context, 4000), ''N/A'') || ''

CRITICAL RULES - FOLLOW ALL:
1. fix_sql MUST be exactly ONE valid Snowflake SQL statement with fully qualified names (ETL_BOT.ETL_SCHEMA.xxx).
2. For procedure fixes: use CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.<correct_name>() RETURNS VARCHAR LANGUAGE SQL AS BEGIN ... END. The procedure name must match the EXISTING procedure that failed (e.g. LOAD_SALES_FACT), NOT the Airflow task_id.
3. For column mismatches: SALES_RAW has PRICE column, SALES_FACT has TOTAL_REVENUE column. Use PRICE AS TOTAL_REVENUE.
4. When inserting into NOT NULL columns from NULLABLE sources: add WHERE clauses (e.g. WHERE CUSTOMER_NAME IS NOT NULL).
5. For misspelled object names: correct the name in the original failing SQL.
6. NEVER use FIXED or TEXT data types. Use NUMBER, VARCHAR, DATE.
7. NEVER generate DROP, DELETE, TRUNCATE, GRANT, or REVOKE.
8. fix_location must specify exactly WHICH object needs the fix.
9. suggested_fix must explain WHERE to apply the fix and WHY.

Respond with ONLY this JSON (no markdown, no backticks):
{"root_cause":"what went wrong","suggested_fix":"where and how to fix including the procedure/object name","fix_sql":"ONE valid Snowflake SQL","fix_location":"ETL_BOT.ETL_SCHEMA.<object_name>","severity":"CRITICAL|HIGH|MEDIUM|LOW","confidence_score":0.0-1.0,"category":"OBJECT_RECREATION|SCHEMA_CHANGE|SQL_SYNTAX|DATA_QUALITY|PERMISSION|CONFIG|TRANSIENT","auto_retry_recommended":true/false}'';

    SELECT SNOWFLAKE.CORTEX.COMPLETE(''mistral-large2'', :v_prompt) INTO v_response;

    v_clean := TRIM(v_response);
    IF (LEFT(v_clean, 7) = ''```json'') THEN
        v_clean := SUBSTR(v_clean, 8);
    END IF;
    IF (LEFT(v_clean, 3) = ''```'') THEN
        v_clean := SUBSTR(v_clean, 4);
    END IF;
    IF (RIGHT(v_clean, 3) = ''```'') THEN
        v_clean := LEFT(v_clean, LENGTH(v_clean) - 3);
    END IF;
    v_clean := TRIM(v_clean);

    INSERT INTO ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS
        (LOG_ID, ROOT_CAUSE, SUGGESTED_FIX, FIX_SQL, SEVERITY, CONFIDENCE_SCORE, CATEGORY, AUTO_RETRY_RECOMMENDED, MODEL_USED)
    SELECT
        :P_LOG_ID,
        TRY_PARSE_JSON(:v_clean):root_cause::VARCHAR,
        TRY_PARSE_JSON(:v_clean):suggested_fix::VARCHAR,
        TRY_PARSE_JSON(:v_clean):fix_sql::VARCHAR,
        TRY_PARSE_JSON(:v_clean):severity::VARCHAR,
        TRY_PARSE_JSON(:v_clean):confidence_score::NUMBER(5,2),
        TRY_PARSE_JSON(:v_clean):category::VARCHAR,
        TRY_PARSE_JSON(:v_clean):auto_retry_recommended::BOOLEAN,
        ''mistral-large2'';

    UPDATE ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS SET IS_ANALYZED = TRUE WHERE LOG_ID = :P_LOG_ID;

    RETURN ''Analyzed LOG_ID='' || :P_LOG_ID || '' | Category: '' || COALESCE(TRY_PARSE_JSON(v_clean):category::VARCHAR, ''UNKNOWN'');
END
';


CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_count INT DEFAULT 0;
    v_lid NUMBER;
    cur1 CURSOR FOR
        SELECT LOG_ID AS LID
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS_STREAM
        WHERE METADATA$ACTION = ''INSERT'';
    cur2 CURSOR FOR
        SELECT LOG_ID AS LID
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS
        WHERE IS_ANALYZED = FALSE
        ORDER BY CREATED_AT DESC;
BEGIN
    FOR rec IN cur1 DO
        v_lid := rec.LID;
        CALL ETL_BOT.ETL_SCHEMA.ANALYZE_ETL_FAILURE_V2(:v_lid);
        v_count := v_count + 1;
    END FOR;
    IF (v_count = 0) THEN
        FOR rec IN cur2 DO
            v_lid := rec.LID;
            CALL ETL_BOT.ETL_SCHEMA.ANALYZE_ETL_FAILURE_V2(:v_lid);
            v_count := v_count + 1;
        END FOR;
    END IF;
    RETURN ''Analyzed '' || v_count || '' new failure(s).'';
END
';


CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.AUTO_APPLY_FIXES_V2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_enabled STRING;
    v_min_conf FLOAT;
    v_max_fixes INT;
    v_allowed_sevs STRING;
    v_applied INT DEFAULT 0;
    v_failed INT DEFAULT 0;
    v_blocked INT DEFAULT 0;
    v_total INT DEFAULT 0;
    v_fix_sql STRING;
    v_category STRING;
    v_sid NUMBER;
    v_lid NUMBER;
    v_dag STRING;
    v_task STRING;
    v_conf STRING;
    v_sev STRING;
    v_result STRING;
    v_fix_status STRING;
    v_error_msg STRING;
BEGIN
    SELECT SETTING_VALUE INTO v_enabled
    FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS WHERE SETTING_KEY = ''AUTO_FIX_ENABLED'';

    IF (v_enabled != ''TRUE'') THEN
        RETURN ''Auto-fix is disabled.'';
    END IF;

    SELECT TRY_CAST(SETTING_VALUE AS FLOAT) INTO v_min_conf
    FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS WHERE SETTING_KEY = ''MIN_CONFIDENCE'';

    SELECT TRY_CAST(SETTING_VALUE AS INT) INTO v_max_fixes
    FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS WHERE SETTING_KEY = ''MAX_AUTO_FIXES_PER_RUN'';

    SELECT SETTING_VALUE INTO v_allowed_sevs
    FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS WHERE SETTING_KEY = ''ALLOWED_SEVERITIES'';

    LET rs RESULTSET := (
        SELECT s.SUGGESTION_ID, s.LOG_ID, s.FIX_SQL, s.CONFIDENCE_SCORE, s.SEVERITY,
               COALESCE(s.CATEGORY, ''UNKNOWN'') AS CATEGORY, l.DAG_ID, l.TASK_ID
        FROM ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s
        JOIN ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l ON s.LOG_ID = l.LOG_ID
        WHERE s.CONFIDENCE_SCORE >= :v_min_conf
          AND CONTAINS(:v_allowed_sevs, s.SEVERITY)
          AND s.FIX_SQL IS NOT NULL
          AND s.FIX_SQL != ''''
          AND s.SUGGESTION_ID NOT IN (
              SELECT SUGGESTION_ID FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE SUGGESTION_ID IS NOT NULL
          )
        ORDER BY
            CASE s.SEVERITY WHEN ''CRITICAL'' THEN 1 WHEN ''HIGH'' THEN 2 WHEN ''MEDIUM'' THEN 3 ELSE 4 END,
            s.CONFIDENCE_SCORE DESC
        LIMIT :v_max_fixes
    );
    LET cur CURSOR FOR rs;

    FOR rec IN cur DO
        v_total := v_total + 1;
        v_sid := rec.SUGGESTION_ID;
        v_lid := rec.LOG_ID;
        v_fix_sql := rec.FIX_SQL;
        v_conf := rec.CONFIDENCE_SCORE;
        v_sev := rec.SEVERITY;
        v_category := rec.CATEGORY;
        v_dag := rec.DAG_ID;
        v_task := rec.TASK_ID;

        CALL ETL_BOT.ETL_SCHEMA.RECREATE_OBJECT(:v_fix_sql, :v_category) INTO v_result;

        IF (v_result = ''SUCCESS'') THEN
            v_fix_status := ''SUCCESS'';
            v_error_msg := NULL;
            v_applied := v_applied + 1;
        ELSEIF (LEFT(v_result, 7) = ''BLOCKED'') THEN
            v_fix_status := ''BLOCKED'';
            v_error_msg := v_result;
            v_blocked := v_blocked + 1;
        ELSE
            v_fix_status := ''FAILED'';
            v_error_msg := v_result;
            v_failed := v_failed + 1;
        END IF;

        INSERT INTO ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
            (SUGGESTION_ID, LOG_ID, DAG_ID, TASK_ID, FIX_SQL, FIX_STATUS, ERROR_MESSAGE, CONFIDENCE_SCORE, SEVERITY, FIX_MODE)
        VALUES
            (:v_sid, :v_lid, :v_dag, :v_task, :v_fix_sql, :v_fix_status, :v_error_msg, :v_conf, :v_sev, ''AUTO'');
    END FOR;

    RETURN ''Auto-fix: '' || v_applied || '' applied, '' || v_failed || '' failed, '' || v_blocked || '' blocked (of '' || v_total || '' total).'';
END;
';

CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.INGEST_FROM_QUERY_HISTORY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_inserted INT DEFAULT 0;
BEGIN
    INSERT INTO ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS
        (DAG_ID, TASK_ID, EXECUTION_DATE, ERROR_MESSAGE, ERROR_TYPE, PIPELINE_CONTEXT, IS_ANALYZED)
    SELECT
        COALESCE(WAREHOUSE_NAME, ''UNKNOWN'') AS DAG_ID,
        COALESCE(LEFT(REGEXP_REPLACE(QUERY_TEXT, ''\\\\s+'', '' ''), 100), ''UNKNOWN'') AS TASK_ID,
        START_TIME AS EXECUTION_DATE,
        ERROR_CODE || '': '' || ERROR_MESSAGE AS ERROR_MESSAGE,
        CASE
            WHEN ERROR_CODE IN (''002003'', ''002141'') THEN ''ObjectNotFound''
            WHEN ERROR_CODE = ''000904'' THEN ''CompilationError''
            WHEN ERROR_CODE IN (''100038'', ''100132'') THEN ''DataError''
            ELSE ''ProgrammingError''
        END AS ERROR_TYPE,
        OBJECT_CONSTRUCT(
            ''query_id'', QUERY_ID,
            ''query_text'', QUERY_TEXT,
            ''user_name'', USER_NAME,
            ''warehouse'', WAREHOUSE_NAME,
            ''database'', DATABASE_NAME,
            ''schema'', SCHEMA_NAME,
            ''error_code'', ERROR_CODE,
            ''start_time'', TO_VARCHAR(START_TIME),
            ''end_time'', TO_VARCHAR(END_TIME)
        )::VARCHAR AS PIPELINE_CONTEXT,
        FALSE AS IS_ANALYZED
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE EXECUTION_STATUS = ''FAIL''
      AND DATABASE_NAME = ''ETL_BOT''
      AND START_TIME >= DATEADD(''HOUR'', -1, CURRENT_TIMESTAMP())
      AND QUERY_ID NOT IN (
          SELECT PARSE_JSON(PIPELINE_CONTEXT):query_id::VARCHAR
          FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS
          WHERE PIPELINE_CONTEXT IS NOT NULL
            AND TRY_PARSE_JSON(PIPELINE_CONTEXT):query_id IS NOT NULL
      )
    ORDER BY START_TIME DESC;

    v_inserted := SQLROWCOUNT;
    RETURN ''Ingested '' || v_inserted || '' failed query(ies) from query history.'';
END;
';


CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.LOAD_SALES_FACT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS ' BEGIN INSERT INTO ETL_BOT.ETL_SCHEMA.SALES_FACT (ORDER_ID, CUSTOMER_NAME, PRODUCT, QUANTITY, TOTAL_REVENUE, ORDER_DATE, REGION) SELECT ORDER_ID, CUSTOMER_NAME, PRODUCT, QUANTITY, PRICE AS TOTAL_REVENUE, ORDER_DATE, REGION FROM ETL_BOT.ETL_SCHEMA.SALES_RAW WHERE CUSTOMER_NAME IS NOT NULL AND PRODUCT IS NOT NULL AND QUANTITY IS NOT NULL AND PRICE IS NOT NULL AND ORDER_DATE IS NOT NULL AND REGION IS NOT NULL; RETURN ''SUCCESS''; END ';



CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.RECREATE_OBJECT("P_FIX_SQL" VARCHAR, "P_CATEGORY" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_upper_sql STRING;
    v_sql STRING;
    v_is_safe BOOLEAN DEFAULT FALSE;
    v_as_pos INT;
    v_before_as STRING;
    v_after_as STRING;
BEGIN
    v_sql := TRIM(:P_FIX_SQL);
    v_upper_sql := UPPER(v_sql);

    IF (v_upper_sql LIKE ''CREATE OR REPLACE PROCEDURE%'' OR v_upper_sql LIKE ''CREATE PROCEDURE%'') THEN
        IF (NOT CONTAINS(v_sql, ''$$'')) THEN
            v_as_pos := POSITION('' AS '' IN UPPER(v_sql));
            IF (v_as_pos > 0) THEN
                v_before_as := LEFT(v_sql, v_as_pos + 3);
                v_after_as := TRIM(SUBSTR(v_sql, v_as_pos + 4));
                IF (RIGHT(v_after_as, 1) = '';'') THEN
                    v_after_as := LEFT(v_after_as, LENGTH(v_after_as) - 1);
                END IF;
                v_sql := v_before_as || '' $$ '' || v_after_as || '' $$'';
            END IF;
        END IF;
        v_is_safe := TRUE;
    ELSEIF (v_upper_sql LIKE ''CREATE OR REPLACE TASK%''
        OR v_upper_sql LIKE ''CREATE TASK%''
        OR v_upper_sql LIKE ''CREATE OR REPLACE STAGE%''
        OR v_upper_sql LIKE ''CREATE STAGE%''
        OR v_upper_sql LIKE ''CREATE OR REPLACE VIEW%''
        OR v_upper_sql LIKE ''CREATE OR REPLACE STREAM%''
        OR v_upper_sql LIKE ''ALTER TASK%'') THEN
        v_is_safe := TRUE;
    ELSEIF (v_upper_sql LIKE ''SELECT%''
        OR v_upper_sql LIKE ''INSERT%''
        OR v_upper_sql LIKE ''CREATE OR REPLACE%''
        OR v_upper_sql LIKE ''ALTER%''
        OR v_upper_sql LIKE ''MERGE%'') THEN
        v_is_safe := TRUE;
    END IF;

    IF (CONTAINS(v_upper_sql, ''DROP DATABASE'')
        OR CONTAINS(v_upper_sql, ''DROP SCHEMA'')
        OR CONTAINS(v_upper_sql, ''DROP TABLE'')
        OR CONTAINS(v_upper_sql, ''TRUNCATE'')
        OR CONTAINS(v_upper_sql, ''DELETE FROM'')
        OR CONTAINS(v_upper_sql, ''GRANT '')
        OR CONTAINS(v_upper_sql, ''REVOKE '')) THEN
        v_is_safe := FALSE;
    END IF;

    IF (NOT v_is_safe) THEN
        RETURN ''BLOCKED: SQL is not safe. Category='' || :P_CATEGORY || '' SQL='' || LEFT(:P_FIX_SQL, 200);
    END IF;

    EXECUTE IMMEDIATE v_sql;
    RETURN ''SUCCESS'';
EXCEPTION
    WHEN OTHER THEN
        RETURN ''FAILED: '' || SQLERRM;
END
';

CREATE OR REPLACE PROCEDURE ETL_BOT.ETL_SCHEMA.WRITE_STREAMLIT_APP()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
def run(session):
    code = r''''''import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

st.set_page_config(page_title="ETL Failure Auto-Fix Bot", layout="wide")
st.title("ETL Failure Auto-Fix Bot")
st.caption("Powered by Snowflake Cortex (mistral-large2)")

date_option = st.radio("Date Filter", ["All Dates", "Today", "Custom Range"], horizontal=True)

if date_option == "All Dates":
    date_filter = "1=1"
elif date_option == "Today":
    today = datetime.now().date()
    date_filter = f"l.CREATED_AT >= ''{today}''::DATE AND l.CREATED_AT < ''{today}''::DATE + INTERVAL ''1 DAY''"
else:
    date_range = st.date_input(
        "Select Range",
        value=(datetime.now().date() - timedelta(days=7), datetime.now().date()),
        max_value=datetime.now().date(),
        key="date_range_picker",
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range[0] if isinstance(date_range, tuple) else date_range
        end_date = datetime.now().date()
    date_filter = f"l.CREATED_AT BETWEEN ''{start_date}''::DATE AND ''{end_date}''::DATE + INTERVAL ''1 DAY''"

auto_msg = ""
if "auto_ran" not in st.session_state:
    st.session_state.auto_ran = False

if not st.session_state.auto_ran:
    unanalyzed_count = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS WHERE IS_ANALYZED = FALSE").collect()[0]["C"]
    if unanalyzed_count > 0:
        with st.spinner(f"Auto-analyzing {unanalyzed_count} new failure(s) and applying fixes..."):
            r1 = session.sql("CALL ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES()").collect()[0][0]
            r2 = session.sql("CALL ETL_BOT.ETL_SCHEMA.AUTO_APPLY_FIXES()").collect()[0][0]
        auto_msg = f"{r1} | {r2}"
    st.session_state.auto_ran = True

if auto_msg:
    st.success(auto_msg)

tab1, tab2, tab3, tab4 = st.tabs(["Dashboard", "Failure Details", "Auto-Fix Results", "Fix History"])

with tab1:
    col1, col2, col3, col4 = st.columns(4)
    total = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE {date_filter}").collect()[0]["C"]
    analyzed = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE IS_ANALYZED = TRUE AND {date_filter}").collect()[0]["C"]
    critical = session.sql(f"""
        SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s
        JOIN ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l ON s.LOG_ID = l.LOG_ID
        WHERE s.SEVERITY = ''CRITICAL'' AND {date_filter}
    """).collect()[0]["C"]
    auto_fixed = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE FIX_STATUS = ''SUCCESS''").collect()[0]["C"]
    col1.metric("Total Failures", total)
    col2.metric("Analyzed by AI", analyzed)
    col3.metric("Critical", critical)
    col4.metric("Auto-Fixed", auto_fixed)

    st.subheader("Failures by Category")
    cat_df = session.sql(f"""
        SELECT COALESCE(s.CATEGORY, ''PENDING'') AS CATEGORY, COUNT(*) AS COUNT
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 2 DESC
    """).to_pandas()
    if not cat_df.empty:
        st.bar_chart(cat_df.set_index("CATEGORY"))

    st.subheader("Failures by Severity")
    sev_df = session.sql(f"""
        SELECT COALESCE(s.SEVERITY, ''PENDING'') AS SEVERITY, COUNT(*) AS COUNT
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 2 DESC
    """).to_pandas()
    if not sev_df.empty:
        st.bar_chart(sev_df.set_index("SEVERITY"))

    st.subheader("Failure Timeline")
    timeline_df = session.sql(f"""
        SELECT DATE_TRUNC(''HOUR'', l.CREATED_AT) AS HOUR, COUNT(*) AS FAILURES
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 1
    """).to_pandas()
    if not timeline_df.empty:
        st.line_chart(timeline_df.set_index("HOUR"))

with tab2:
    st.subheader("All Failures with AI Diagnosis")
    detail_df = session.sql(f"""
        SELECT
            l.LOG_ID, l.DAG_ID, l.TASK_ID, l.ERROR_TYPE, l.CREATED_AT,
            l.ERROR_MESSAGE,
            s.ROOT_CAUSE, s.SEVERITY, s.CATEGORY, s.FIX_SQL,
            s.CONFIDENCE_SCORE, s.SUGGESTED_FIX
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        ORDER BY l.CREATED_AT DESC
        LIMIT 100
    """).to_pandas()

    if not detail_df.empty:
        severity_filter = st.multiselect("Filter by Severity", options=detail_df["SEVERITY"].dropna().unique().tolist(), key="sev_filter")
        if severity_filter:
            detail_df = detail_df[detail_df["SEVERITY"].isin(severity_filter)]

        for _, row in detail_df.iterrows():
            sev = row.get("SEVERITY", "PENDING") or "PENDING"
            color = {"CRITICAL": "red", "HIGH": "orange", "MEDIUM": "blue", "LOW": "green"}.get(sev, "gray")
            with st.expander(f":{color}[{sev}] {row[''DAG_ID'']} / {row[''TASK_ID'']}"):
                st.markdown(f"**Error Type:** {row[''ERROR_TYPE'']}")
                st.markdown(f"**Error Message:** {row.get(''ERROR_MESSAGE'', ''N/A'')}")
                st.markdown("---")
                st.markdown(f"**Root Cause:** {row.get(''ROOT_CAUSE'', ''Pending analysis...'')}")
                st.markdown(f"**Suggested Fix:** {row.get(''SUGGESTED_FIX'', ''N/A'')}")
                st.markdown(f"**Category:** {row.get(''CATEGORY'', ''N/A'')}")
                st.markdown(f"**Confidence:** {row.get(''CONFIDENCE_SCORE'', ''N/A'')}")
                if row.get("FIX_SQL"):
                    st.markdown("**Fix SQL:**")
                    st.code(row["FIX_SQL"], language="sql")
    else:
        st.info("No failures found in the selected date range.")

with tab3:
    st.subheader("Auto-Fix Results")
    st.markdown("Fixes are applied automatically for **high-confidence** issues. The bot runs every 5 minutes via Snowflake Tasks.")

    col_s, col_f, col_b = st.columns(3)
    success = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE FIX_STATUS = ''SUCCESS''").collect()[0]["C"]
    failed = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE FIX_STATUS = ''FAILED''").collect()[0]["C"]
    blocked = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE FIX_STATUS = ''BLOCKED''").collect()[0]["C"]
    col_s.metric("Successfully Fixed", success)
    col_f.metric("Failed", failed)
    col_b.metric("Blocked (Destructive)", blocked)

    st.subheader("Recent Auto-Fixes")
    fixes_df = session.sql("""
        SELECT
            h.FIX_STATUS,
            h.DAG_ID,
            h.TASK_ID,
            h.SEVERITY,
            h.CONFIDENCE_SCORE,
            LEFT(h.FIX_SQL, 500) AS FIX_SQL,
            h.ERROR_MESSAGE AS FIX_ERROR,
            h.APPLIED_AT
        FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        ORDER BY h.APPLIED_AT DESC
        LIMIT 50
    """).to_pandas()

    if not fixes_df.empty:
        for _, row in fixes_df.iterrows():
            status = row["FIX_STATUS"]
            if status == "SUCCESS":
                icon = ":green[SUCCESS]"
            elif status == "BLOCKED":
                icon = ":orange[BLOCKED]"
            else:
                icon = ":red[FAILED]"
            with st.expander(f"{icon} -- {row[''DAG_ID'']} / {row[''TASK_ID'']} -- {row[''APPLIED_AT'']}"):
                st.markdown(f"**Severity:** {row[''SEVERITY'']}")
                st.markdown(f"**Confidence:** {row[''CONFIDENCE_SCORE'']}")
                st.markdown("**Fix SQL Applied:**")
                st.code(row["FIX_SQL"], language="sql")
                if row.get("FIX_ERROR"):
                    st.error(f"Error: {row[''FIX_ERROR'']}")
    else:
        st.info("No auto-fixes applied yet.")

with tab4:
    st.subheader("Full Fix History")
    status_filter = st.multiselect("Filter by Status", ["SUCCESS", "FAILED", "BLOCKED"], default=["SUCCESS", "FAILED", "BLOCKED"], key="hist_filter")
    status_in = ",".join([f"''{s}''" for s in status_filter]) if status_filter else "''SUCCESS'',''FAILED'',''BLOCKED''"

    history_df = session.sql(f"""
        SELECT FIX_ID, FIX_STATUS, FIX_MODE, DAG_ID, TASK_ID, SEVERITY,
               CONFIDENCE_SCORE, FIX_SQL, ERROR_MESSAGE, APPLIED_AT
        FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
        WHERE FIX_STATUS IN ({status_in})
        ORDER BY APPLIED_AT DESC
        LIMIT 100
    """).to_pandas()

    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True)
    else:
        st.info("No fix history found.")

    st.subheader("Auto-Fix Settings")
    settings_df = session.sql("SELECT * FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_SETTINGS ORDER BY SETTING_KEY").to_pandas()
    st.dataframe(settings_df, use_container_width=True)
''''''
    import os
    file_path = os.path.join(''/tmp'', ''etl_bot_dashboard.py'')
    with open(file_path, ''w'') as f:
        f.write(code)
    session.file.put(file_path, ''@ETL_BOT.ETL_SCHEMA.STREAMLIT_STAGE'', auto_compress=False, overwrite=True)
    os.unlink(file_path)
    return ''Streamlit app updated successfully''
';
