# Upsert transaction balance into postgres
def upsert_transaction_ledger(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            transaction_ledger (transaction_id, transaction_timestamp, transaction_amount, fund_id, deal_id, kafka_timestamp, kafka_partition, kafka_offset, kafka_topic, last_modified)
            select
                transaction_id,
                transaction_timestamp,
                signed_amount,
                fund_id,
                deal_id,
                kafka_timestamp,
                kafka_partition,
                kafka_offset,
                kafka_topic,
                now()
            from transaction_ledger_staging
        ON CONFLICT (transaction_id) DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="transaction_ledger_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS transaction_ledger_staging", jdbc_url, jdbc_props)

def upsert_transaction_balance(jdbc_url, jdbc_props):
    # Upsert into canonical table
    sql_text = """
        INSERT 
            INTO transaction_balance (fund_id, deal_id, balance, last_modified)
            SELECT 
                fund_id, 
                deal_id, 
                SUM(transaction_amount), 
                NOW()
            FROM transaction_ledger
        GROUP BY 
            fund_id, 
            deal_id
        ON CONFLICT (fund_id, deal_id)
        DO UPDATE SET
            balance = EXCLUDED.balance,
            last_modified = NOW()
    """

    # Upsert into canonical table
    execute_sql(sql_text, jdbc_url, jdbc_props)


# Upsert late arriving event log into postgres
def upsert_late_arriving_events(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            late_arriving_event_log (surrogate_pk, run_id, transaction_id, txn_age_sec, transaction_timestamp, event_timestamp, transaction_amount, currency, transaction_type, status, fund_id, fund_name, deal_id, deal_name, kafka_timestamp, kafka_partition, kafka_offset, kafka_topic)
            select
                surrogate_pk,
                run_id,
                transaction_id,
                txn_age_sec,
                transaction_timestamp,
                event_timestamp,
                transaction_amount,
                currency,
                transaction_type,
                status,
                fund_id,
                fund_name,
                deal_id,
                deal_name,
                kafka_timestamp,
                kafka_partition,
                kafka_offset,
                kafka_topic
            from late_arriving_event_staging
        ON CONFLICT (surrogate_pk)
        DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="late_arriving_event_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS late_arriving_event_staging", jdbc_url, jdbc_props)


# Upsert duplicate event log into postgres
def upsert_duplicate_events(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            duplicate_records_log (surrogate_pk, run_id, transaction_id, duplicate_count, earliest_transaction_timestamp, latest_transaction_timestamp, winner_transaction_timestamp, winner_kafka_timestamp, winner_partition, winner_offset, winner_topic)
            select
                surrogate_pk, 
                run_id, 
                transaction_id, 
                duplicate_count, 
                earliest_transaction_timestamp, 
                latest_transaction_timestamp, 
                winner_transaction_timestamp, 
                winner_kafka_timestamp, 
                winner_partition, 
                winner_offset, 
                winner_topic
            from duplicate_records_staging
        ON CONFLICT (surrogate_pk)
        DO NOTHING
    """
    try:
        df.write.jdbc(url=jdbc_url, table="duplicate_records_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS duplicate_records_staging", jdbc_url, jdbc_props)

# Upsert run metrics into postgres
def upsert_run_metrics(df, jdbc_url, jdbc_props):
    sql_text = """
        INSERT INTO
            run_metrics (run_id, record_count, duplicate_count, late_arrival_count, run_timestamp)
            select
                run_id,
                record_count,
                duplicate_count,
                late_arrival_count,
                run_timestamp
            from run_metrics_staging
        ON CONFLICT (run_id)
        DO UPDATE SET
            record_count = EXCLUDED.record_count,
            duplicate_count = EXCLUDED.duplicate_count,
            late_arrival_count = EXCLUDED.late_arrival_count,
            run_timestamp = EXCLUDED.run_timestamp
    """
    try:
        df.write.jdbc(url=jdbc_url, table="run_metrics_staging", mode="overwrite", properties=jdbc_props)
        execute_sql(sql_text, jdbc_url, jdbc_props)
    except Exception:
        raise
    finally:
        execute_sql("DROP TABLE IF EXISTS run_metrics_staging", jdbc_url, jdbc_props)