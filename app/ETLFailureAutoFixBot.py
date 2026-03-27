import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

st.title("ETL Failure Auto-Fix Bot")
st.caption("Powered by Snowflake Cortex (mistral-large2)")

date_option = st.radio("Date Filter", ["Today", "All Dates", "Custom Range"], horizontal=True)

if date_option == "All Dates":
    date_filter = "1=1"
    fix_date_filter = "1=1"
    date_label = "all time"
elif date_option == "Today":
    today = datetime.now().date()
    date_filter = f"l.CREATED_AT >= '{today}'::DATE AND l.CREATED_AT < '{today}'::DATE + INTERVAL '1 DAY'"
    fix_date_filter = f"h.APPLIED_AT >= '{today}'::DATE AND h.APPLIED_AT < '{today}'::DATE + INTERVAL '1 DAY'"
    date_label = f"today ({today})"
else:
    date_range = st.date_input(
        "Select Range",
        value=(datetime.now().date() - timedelta(days=7), datetime.now().date()),
        max_value=datetime.now().date(),
        key="date_range_picker"
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range[0] if isinstance(date_range, (list, tuple)) else date_range
        end_date = datetime.now().date()
    date_filter = f"l.CREATED_AT BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE + INTERVAL '1 DAY'"
    fix_date_filter = f"h.APPLIED_AT BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE + INTERVAL '1 DAY'"
    date_label = f"{start_date} to {end_date}"

auto_msg = ""
if "auto_ran" not in st.session_state:
    st.session_state.auto_ran = False

if not st.session_state.auto_ran:
    unanalyzed_count = session.sql("SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS WHERE IS_ANALYZED = FALSE").collect()[0]["C"]
    unapplied_count = session.sql("""
        SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s
        WHERE s.FIX_SQL IS NOT NULL
          AND s.SUGGESTION_ID NOT IN (SELECT SUGGESTION_ID FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY WHERE SUGGESTION_ID IS NOT NULL)
    """).collect()[0]["C"]

    if unanalyzed_count > 0 or unapplied_count > 0:
        with st.spinner(f"Analyzing {unanalyzed_count} pending failure(s) and preparing {unapplied_count} suggested fix(es) for review..."):
            if unanalyzed_count > 0:
                r1 = session.sql("CALL ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES()").collect()[0][0]
            else:
                r1 = "No new failures to analyze"
            r2 = "Fixes queued for human approval"
        auto_msg = f"{r1} | {r2}"
    st.session_state.auto_ran = True
if auto_msg:
    st.success(auto_msg)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Dashboard",
    "Failure Details",
    "Auto-Fix Results",
    "Fix History & Settings",
    "Talk to Fix Bot",
    "Notifications"
])

with tab1:
    col1, col2, col3, col4 = st.columns(4)
    total = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE {date_filter}").collect()[0]["C"]
    analyzed = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE IS_ANALYZED = TRUE AND {date_filter}").collect()[0]["C"]
    critical = session.sql(f"""
        SELECT COUNT(DISTINCT l.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s
        JOIN ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l ON s.LOG_ID = l.LOG_ID
        WHERE s.SEVERITY = 'CRITICAL' AND {date_filter}
    """).collect()[0]["C"]
    auto_fixed = session.sql(f"""
        SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        WHERE h.FIX_STATUS = 'SUCCESS' AND {fix_date_filter}
    """).collect()[0]["C"]
    col1.metric("Total Failures", total)
    col2.metric("Analyzed by AI", analyzed)
    col3.metric("Critical", critical)
    col4.metric("Auto-Fixed", auto_fixed)

    st.subheader("Failures by Category")
    cat_df = session.sql(f"""
        SELECT COALESCE(s.CATEGORY, 'Unanalyzed') AS CATEGORY, COUNT(DISTINCT l.LOG_ID) AS COUNT
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 2 DESC
    """).to_pandas()
    if not cat_df.empty:
        st.bar_chart(cat_df.set_index("CATEGORY"))

    st.subheader("Failures by Severity")
    sev_df = session.sql(f"""
        SELECT COALESCE(s.SEVERITY, 'Unknown') AS SEVERITY, COUNT(DISTINCT l.LOG_ID) AS COUNT
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 2 DESC
    """).to_pandas()
    if not sev_df.empty:
        st.bar_chart(sev_df.set_index("SEVERITY"))

    st.subheader("Failure Timeline")
    timeline_df = session.sql(f"""
        SELECT DATE_TRUNC('HOUR', l.CREATED_AT) AS HOUR, COUNT(*) AS FAILURES
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        WHERE {date_filter}
        GROUP BY 1 ORDER BY 1
    """).to_pandas()
    if not timeline_df.empty:
        st.line_chart(timeline_df.set_index("HOUR"))

    st.divider()
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Ingest from Query History", key="ingest_btn"):
            with st.spinner("Pulling failed queries from Snowflake Query History..."):
                result = session.sql("CALL ETL_BOT.ETL_SCHEMA.INGEST_FROM_QUERY_HISTORY()").collect()[0][0]
            st.success(result)
            st.experimental_rerun()
    with col_b:
        if st.button("Re-Analyze All Failures", key="reanalyze_btn"):
            with st.spinner("Re-analyzing failures..."):
                r1 = session.sql("CALL ETL_BOT.ETL_SCHEMA.ANALYZE_NEW_FAILURES()").collect()[0][0]
            st.success(r1)
            st.experimental_rerun()

with tab2:
    st.subheader("All Failures with AI Diagnosis")
    detail_df = session.sql(f"""
        SELECT
            l.LOG_ID, l.DAG_ID, l.TASK_ID, l.ERROR_TYPE, l.EXECUTION_DATE,
            l.ERROR_MESSAGE,
            s.ROOT_CAUSE, s.SUGGESTED_FIX, s.FIX_SQL, s.SEVERITY,
            s.CONFIDENCE_SCORE, s.CATEGORY
        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
        WHERE {date_filter}
        ORDER BY l.CREATED_AT DESC
        LIMIT 100
    """).to_pandas()

    if not detail_df.empty:
        severity_filter = st.multiselect("Filter by Severity", detail_df["SEVERITY"].dropna().unique().tolist(), key="sev_filter")
        if severity_filter:
            detail_df = detail_df[detail_df["SEVERITY"].isin(severity_filter)]

        for _, row in detail_df.iterrows():
            sev = row.get("SEVERITY", "Unknown") or "Unknown"
            color = {"CRITICAL": "red", "HIGH": "orange", "MEDIUM": "blue"}.get(sev, "gray")
            with st.expander(f":{color}[{sev}] {row['DAG_ID']} / {row['TASK_ID']}"):
                st.markdown(f"**Error Type:** {row['ERROR_TYPE']}")
                st.markdown(f"**Error Message:** {row.get('ERROR_MESSAGE', 'N/A')}")
                st.markdown("---")
                st.markdown(f"**Root Cause:** {row.get('ROOT_CAUSE', 'Pending analysis')}")
                st.markdown(f"**Suggested Fix:** {row.get('SUGGESTED_FIX', 'Pending')}")
                st.markdown(f"**Category:** {row.get('CATEGORY', 'N/A')}")
                st.markdown(f"**Confidence:** {row.get('CONFIDENCE_SCORE', 'N/A')}")
                if row.get("FIX_SQL"):
                    st.markdown("**AI-Suggested Fix SQL:**")
                    st.code(row["FIX_SQL"], language="sql")

                    already_applied = session.sql(f"""
                        SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
                        WHERE LOG_ID = {row['LOG_ID']} AND FIX_STATUS = 'SUCCESS'
                    """).collect()[0]["C"]

                    if already_applied > 0:
                        st.success("Fix already applied successfully.")
                    else:
                        st.warning("This fix requires your approval before it can be applied.")
                        if st.button(f"Approve & Apply Fix", key=f"approve_{row['LOG_ID']}"):
                            with st.spinner("Applying approved fix..."):
                                try:
                                    session.sql(row["FIX_SQL"]).collect()
                                    session.sql(f"""
                                        INSERT INTO ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
                                        (SUGGESTION_ID, LOG_ID, DAG_ID, TASK_ID, FIX_SQL, FIX_STATUS, CONFIDENCE_SCORE, SEVERITY, FIX_MODE)
                                        SELECT s.SUGGESTION_ID, s.LOG_ID, l.DAG_ID, l.TASK_ID, s.FIX_SQL, 'SUCCESS',
                                               s.CONFIDENCE_SCORE, s.SEVERITY, 'MANUAL'
                                        FROM ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s
                                        JOIN ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l ON s.LOG_ID = l.LOG_ID
                                        WHERE s.LOG_ID = {row['LOG_ID']}
                                    """).collect()
                                    st.success("Fix applied successfully!")
                                    st.experimental_rerun()
                                except Exception as e:
                                    session.sql(f"""
                                        INSERT INTO ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
                                        (LOG_ID, DAG_ID, TASK_ID, FIX_SQL, FIX_STATUS, ERROR_MESSAGE, FIX_MODE)
                                        VALUES ({row['LOG_ID']}, '{row['DAG_ID']}', '{row['TASK_ID']}',
                                                '{row["FIX_SQL"].replace(chr(39), chr(39)+chr(39))}', 'FAILED',
                                                '{str(e).replace(chr(39), chr(39)+chr(39))}', 'MANUAL')
                                    """).collect()
                                    st.error(f"Fix failed: {e}")
    else:
        st.info("No failures found in the selected date range.")

with tab3:
    st.subheader("Auto-Fix Results")
    st.markdown("Fixes now require **human approval** before being applied. Review pending fixes in the **Failure Details** tab.")

    col_s, col_f, col_b = st.columns(3)
    success = session.sql(f"""
        SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        WHERE h.FIX_STATUS = 'SUCCESS' AND {fix_date_filter}
    """).collect()[0]["C"]
    failed = session.sql(f"""
        SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        WHERE h.FIX_STATUS = 'FAILED' AND {fix_date_filter}
    """).collect()[0]["C"]
    blocked = session.sql(f"""
        SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        WHERE h.FIX_STATUS = 'BLOCKED' AND {fix_date_filter}
    """).collect()[0]["C"]
    col_s.metric("Successfully Fixed", success)
    col_f.metric("Failed", failed)
    col_b.metric("Blocked (Destructive)", blocked)

    st.subheader("Recent Fixes")
    fixes_df = session.sql(f"""
        SELECT
            h.FIX_ID, h.DAG_ID, h.TASK_ID, h.FIX_STATUS, h.SEVERITY,
            h.CONFIDENCE_SCORE, h.FIX_MODE, h.APPLIED_AT,
            LEFT(h.FIX_SQL, 500) AS FIX_SQL, h.ERROR_MESSAGE AS FIX_ERROR
        FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h
        WHERE {fix_date_filter}
        ORDER BY h.APPLIED_AT DESC
        LIMIT 50
    """).to_pandas()

    if not fixes_df.empty:
        for _, row in fixes_df.iterrows():
            status = row["FIX_STATUS"]
            mode_label = f" [{row.get('FIX_MODE', 'AUTO')}]" if row.get("FIX_MODE") else ""
            if status == "SUCCESS":
                icon = f":green[SUCCESS{mode_label}]"
            elif status == "BLOCKED":
                icon = f":orange[BLOCKED{mode_label}]"
            else:
                icon = f":red[FAILED{mode_label}]"
            with st.expander(f"{icon} -- {row['DAG_ID']} / {row['TASK_ID']} -- {row['APPLIED_AT']}"):
                st.markdown(f"**Severity:** {row['SEVERITY']}")
                st.markdown(f"**Confidence:** {row['CONFIDENCE_SCORE']}")
                st.markdown(f"**Fix Mode:** {row.get('FIX_MODE', 'AUTO')}")
                st.markdown("**Fix SQL Applied:**")
                st.code(row["FIX_SQL"], language="sql")
                if row.get("FIX_ERROR"):
                    st.error(f"Error: {row['FIX_ERROR']}")
    else:
        st.info("No fixes applied yet.")

with tab4:
    st.subheader("Full Fix History")
    status_filter = st.multiselect("Filter by Status", ["SUCCESS", "FAILED", "BLOCKED"], default=["SUCCESS", "FAILED", "BLOCKED"], key="hist_status")
    status_in = ", ".join([f"'{s}'" for s in status_filter])

    history_df = session.sql(f"""
        SELECT FIX_ID, SUGGESTION_ID, LOG_ID, DAG_ID, TASK_ID, FIX_STATUS, FIX_MODE,
               CONFIDENCE_SCORE, SEVERITY, APPLIED_AT
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

with tab5:
    st.subheader("Talk to the Fix Bot")
    st.markdown("Ask questions about your ETL failures, diagnostics, and fix history. The AI will analyze your data and respond.")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            st.markdown(f"**You:** {msg['content']}")
        else:
            st.markdown(f"**Fix Bot:** {msg['content']}")
        st.markdown("---")

    with st.form(key="chat_form", clear_on_submit=True):
        user_input = st.text_input("Ask about your ETL failures...", placeholder="e.g., How many failures in the last 3 months?")
        submitted = st.form_submit_button("Ask Fix Bot")

    if submitted and user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})

        with st.spinner("Analyzing..."):
            total_failures = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE {date_filter}").collect()[0]["C"]
            analyzed_count = session.sql(f"SELECT COUNT(*) AS C FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l WHERE IS_ANALYZED = TRUE AND {date_filter}").collect()[0]["C"]
            fixes_applied = session.sql(f"SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h WHERE h.FIX_STATUS = 'SUCCESS' AND {fix_date_filter}").collect()[0]["C"]
            fixes_failed = session.sql(f"SELECT COUNT(DISTINCT h.LOG_ID) AS C FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY h WHERE h.FIX_STATUS = 'FAILED' AND {fix_date_filter}").collect()[0]["C"]

            recent_failures = session.sql(f"""
                SELECT l.DAG_ID, l.TASK_ID, l.ERROR_TYPE, l.ERROR_MESSAGE,
                       s.ROOT_CAUSE, s.SEVERITY, s.CATEGORY, l.CREATED_AT
                FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
                LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
                WHERE {date_filter}
                ORDER BY l.CREATED_AT DESC LIMIT 20
            """).to_pandas()

            context_str = f"""You are an ETL failure diagnostic assistant. The user is viewing data for: {date_label}. Here is the current state:
- Total failures logged: {total_failures}
- Failures analyzed by AI: {analyzed_count}
- Successful fixes applied: {fixes_applied}
- Failed fix attempts: {fixes_failed}

Recent failures (up to 20):
{recent_failures.to_string(index=False) if not recent_failures.empty else 'No recent failures.'}

Answer the user's question based on this data. Be concise and helpful. If the question requires data you don't have, say so."""

            prompt = f"{context_str}\n\nUser question: {user_input}"
            prompt_escaped = prompt.replace("'", "''")

            response = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt_escaped}') AS RESPONSE
            """).collect()[0]["RESPONSE"]

            st.session_state.chat_history.append({"role": "assistant", "content": response})
            st.experimental_rerun()

with tab6:
    st.subheader("Notification Settings")
    st.markdown("""
    Configure email notifications to alert your team when ETL failures occur.
    This requires a **Notification Integration** to be set up in Snowflake.
    """)

    st.info("""
    **Setup Instructions:**
    1. Create a notification integration:
    ```sql
    CREATE OR REPLACE NOTIFICATION INTEGRATION etl_failure_alerts
      TYPE = EMAIL
      ENABLED = TRUE
      ALLOWED_RECIPIENTS = ('your-team@company.com');
    ```
    2. Enter the integration name and recipient email below.
    """)

    col_n1, col_n2 = st.columns(2)
    with col_n1:
        notif_integration = st.text_input("Notification Integration Name", value="etl_failure_alerts", key="notif_int")
    with col_n2:
        recipient_email = st.text_input("Recipient Email", placeholder="team@company.com", key="notif_email")

    if st.button("Send Test Notification", key="test_notif"):
        if recipient_email:
            with st.spinner("Sending test email..."):
                try:
                    session.sql(f"""
                        CALL SYSTEM$SEND_EMAIL(
                            '{notif_integration}',
                            '{recipient_email}',
                            'ETL Bot - Test Notification',
                            'This is a test notification from the ETL Failure Auto-Fix Bot. Notifications are working correctly.'
                        )
                    """).collect()
                    st.success(f"Test email sent to {recipient_email}!")
                except Exception as e:
                    st.error(f"Failed to send email: {e}")
        else:
            st.warning("Please enter a recipient email address.")

    st.divider()

    st.subheader("Send Failure Alert Now")
    st.markdown("Sends a summary of recent unresolved failures to the configured email.")

    if st.button("Send Failure Summary Email", key="send_failure_alert"):
        if recipient_email:
            with st.spinner("Generating failure summary and sending..."):
                try:
                    summary_df = session.sql("""
                        SELECT l.DAG_ID, l.TASK_ID, l.ERROR_TYPE, s.SEVERITY,
                               s.ROOT_CAUSE, l.CREATED_AT
                        FROM ETL_BOT.ETL_SCHEMA.ETL_FAILURE_LOGS l
                        LEFT JOIN ETL_BOT.ETL_SCHEMA.ETL_FIX_SUGGESTIONS s ON l.LOG_ID = s.LOG_ID
                        WHERE l.LOG_ID NOT IN (
                            SELECT LOG_ID FROM ETL_BOT.ETL_SCHEMA.AUTO_FIX_HISTORY
                            WHERE FIX_STATUS = 'SUCCESS'
                        )
                        ORDER BY l.CREATED_AT DESC
                        LIMIT 20
                    """).to_pandas()

                    if not summary_df.empty:
                        body_lines = ["ETL Failure Auto-Fix Bot - Unresolved Failures Report", "=" * 50, ""]
                        for _, r in summary_df.iterrows():
                            body_lines.append(f"DAG: {r['DAG_ID']} | Task: {r['TASK_ID']}")
                            body_lines.append(f"  Severity: {r.get('SEVERITY', 'N/A')} | Type: {r['ERROR_TYPE']}")
                            body_lines.append(f"  Root Cause: {r.get('ROOT_CAUSE', 'Pending analysis')}")
                            body_lines.append(f"  Time: {r['CREATED_AT']}")
                            body_lines.append("")
                        body = chr(10).join(body_lines)
                    else:
                        body = "No unresolved ETL failures found. All systems healthy!"

                    body_escaped = body.replace("'", "''")
                    session.sql(f"""
                        CALL SYSTEM$SEND_EMAIL(
                            '{notif_integration}',
                            '{recipient_email}',
                            'ETL Bot - Failure Alert Summary',
                            '{body_escaped}'
                        )
                    """).collect()
                    st.success(f"Failure summary sent to {recipient_email}!")
                except Exception as e:
                    st.error(f"Failed to send alert: {e}")
        else:
            st.warning("Please enter a recipient email address.")
