# db_manager.py
import psycopg2
import psycopg2.extras
import json
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            return True
        except psycopg2.OperationalError:
            return False

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def check_and_create_tables(self):
        """Checks if required tables exist and creates them if they don't, including SyncStatus."""
        # --- THIS IS THE CRITICAL FIX FOR THE SYNTAX ERROR ---
        commands = (
            """
            CREATE TABLE IF NOT EXISTS documents (
                uuid VARCHAR(255) PRIMARY KEY, submission_uuid VARCHAR(255), internal_id VARCHAR(255),
                type_name VARCHAR(50), issuer_id VARCHAR(50), issuer_name VARCHAR(255),
                receiver_id VARCHAR(50), receiver_name VARCHAR(255), date_time_issued TIMESTAMP,
                date_time_received TIMESTAMP, total_sales NUMERIC, total_discount NUMERIC,
                net_amount NUMERIC, total_amount NUMERIC, status VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS document_lines (
                id SERIAL PRIMARY KEY, document_uuid VARCHAR(255) REFERENCES documents(uuid) ON DELETE CASCADE,
                description TEXT, item_code VARCHAR(255), quantity NUMERIC, unit_value_amount_egp NUMERIC,
                sales_total NUMERIC, net_total NUMERIC, total NUMERIC
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sent_documents (
                uuid VARCHAR(255) PRIMARY KEY, submission_uuid VARCHAR(255), internal_id VARCHAR(255),
                type_name VARCHAR(50), issuer_id VARCHAR(50), issuer_name VARCHAR(255),
                receiver_id VARCHAR(50), receiver_name VARCHAR(255), date_time_issued TIMESTAMP,
                date_time_received TIMESTAMP, total_sales NUMERIC, total_discount NUMERIC,
                net_amount NUMERIC, total_amount NUMERIC, status VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sent_document_lines (
                id SERIAL PRIMARY KEY, document_uuid VARCHAR(255) REFERENCES sent_documents(uuid) ON DELETE CASCADE,
                description TEXT, item_code VARCHAR(255), quantity NUMERIC, unit_value_amount_egp NUMERIC,
                sales_total NUMERIC, net_total NUMERIC, total NUMERIC
            )
            """,
            # --- NEW SYNC STATUS TABLE ---
           """
            CREATE TABLE IF NOT EXISTS SyncStatus (
                id SERIAL PRIMARY KEY,
                client_id VARCHAR(255) UNIQUE NOT NULL,
                last_sync_timestamp TIMESTAMP NOT NULL,
                last_synced_uuid VARCHAR(255),      -- NEW
                last_synced_internal_id VARCHAR(255) -- NEW
            );
            """
        )
        try:
            with self.conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
                self.conn.commit()
            print("Schema verification complete. All tables are ready.")
            return True
        except psycopg2.Error as e:
            print(f"Failed to create or verify tables: {e}")
            self.conn.rollback()
            return False

    def get_all_sync_statuses(self):
        """Fetches the last sync details for all clients."""
        statuses = {}
        sql = "SELECT client_id, last_sync_timestamp, last_synced_uuid, last_synced_internal_id FROM SyncStatus;"
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    # Return a tuple with all the info
                    statuses[row[0]] = (row[1], row[2], row[3]) 
            return statuses
        except psycopg2.Error as e:
            print(f"Failed to get sync statuses: {e}")
            return {}

    def update_sync_status(self, client_id, sync_timestamp, uuid, internal_id):
        """Records the full details of the last successful sync for a client."""
        sql = """
            INSERT INTO SyncStatus (client_id, last_sync_timestamp, last_synced_uuid, last_synced_internal_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (client_id) DO UPDATE 
            SET last_sync_timestamp = EXCLUDED.last_sync_timestamp,
                last_synced_uuid = EXCLUDED.last_synced_uuid,
                last_synced_internal_id = EXCLUDED.last_synced_internal_id;
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (client_id, sync_timestamp, uuid, internal_id))
            self.conn.commit()
            print(f"Updated sync status for {client_id} to doc {uuid}")
        except psycopg2.Error as e:
            print(f"Failed to update sync status: {e}")
            self.conn.rollback()
            
    # --- document_exists and insert_document methods from the previous version remain the same ---
    def document_exists(self, uuid, table_prefix=""):
        table_name = f"{table_prefix}documents"; query = f"SELECT 1 FROM {table_name} WHERE uuid = %s"
        with self.conn.cursor() as cur: cur.execute(query, (uuid,)); return cur.fetchone() is not None
    
    def insert_document(self, doc_data, table_prefix=""):
        """
        Inserts a single document, correctly parsing BOTH the normal nested structure
        and the flat structure used for 'Cancelled' documents. Also handles API typos.
        """
        header_table = f"{table_prefix}documents"
        lines_table = f"{table_prefix}document_lines"
        
        try:
            with self.conn.cursor() as cur:
                # --- THIS IS THE CRITICAL FIX ---
                # Check if the 'document' key exists. If so, use it as the source.
                # If not (like in a 'Cancelled' doc), use the top-level doc_data itself.
                doc_header = doc_data.get('document', doc_data)

                # Now, we extract data from the determined source object (doc_header)
                header_data = {
                    "uuid": doc_data.get('uuid'),
                    "submission_uuid": doc_data.get('submissionUUID'),
                    "internal_id": doc_header.get('internalID'), # Note: 'internalID' not 'internalId' in the new JSON
                    "type_name": doc_header.get('documentType'),
                    "issuer_id": doc_header.get('issuer', {}).get('id'),
                    "issuer_name": doc_header.get('issuer', {}).get('name'),
                    "receiver_id": doc_header.get('receiver', {}).get('id'),
                    "receiver_name": doc_header.get('receiver', {}).get('name'),
                    "date_time_issued": doc_header.get('dateTimeIssued'),
                    # TYPO FIX: Check for both 'dateTimeReceived' and the API's typo 'dateTimeRecevied'
                    "date_time_received": doc_data.get('dateTimeReceived') or doc_data.get('dateTimeRecevied'),
                    "total_sales": doc_header.get('totalSales'),
                    "total_discount": doc_header.get('totalDiscount'),
                    "net_amount": doc_header.get('netAmount'),
                    "total_amount": doc_header.get('totalAmount'),
                    "status": doc_data.get('status')
                }
                
                header_sql = f"""
                    INSERT INTO {header_table} (
                        uuid, submission_uuid, internal_id, type_name, issuer_id, issuer_name, 
                        receiver_id, receiver_name, date_time_issued, date_time_received, 
                        total_sales, total_discount, net_amount, total_amount, status
                    ) VALUES (
                        %(uuid)s, %(submission_uuid)s, %(internal_id)s, %(type_name)s, %(issuer_id)s, %(issuer_name)s, 
                        %(receiver_id)s, %(receiver_name)s, %(date_time_issued)s, %(date_time_received)s, 
                        %(total_sales)s, %(total_discount)s, %(net_amount)s, %(total_amount)s, %(status)s
                    );
                """
                cur.execute(header_sql, header_data)

                # The invoice lines are always at the top level
                for line in doc_data.get('invoiceLines', []):
                    line_data = {
                        "document_uuid": doc_data.get('uuid'),
                        "description": line.get('description'),
                        "item_code": line.get('itemCode'),
                        "quantity": line.get('quantity'),
                        "unit_value_amount_egp": line.get('unitValue', {}).get('amountEGP'),
                        "sales_total": line.get('salesTotal'),
                        "net_total": line.get('netTotal'),
                        "total": line.get('total')
                    }
                    lines_sql = f"""
                        INSERT INTO {lines_table} (
                            document_uuid, description, item_code, quantity, 
                            unit_value_amount_egp, sales_total, net_total, total
                        ) VALUES (
                            %(document_uuid)s, %(description)s, %(item_code)s, %(quantity)s, 
                            %(unit_value_amount_egp)s, %(sales_total)s, %(net_total)s, %(total)s
                        );
                    """
                    cur.execute(lines_sql, line_data)
                
            self.conn.commit()
            return (True, "Success")

        except (psycopg2.Error, ValueError) as e:
            error_message = f"DB Error on doc {doc_data.get('uuid')}: {e}"
            print(error_message)
            self.conn.rollback()
            return (False, str(e).strip())

    def get_latest_invoice_timestamp(self, client_id):
        """
        Finds the most recent 'date_time_received' for a client across both
        received (documents) and sent (sent_documents) tables.
        """
        latest_ts = None
        queries = [
            "SELECT MAX(date_time_received) FROM documents WHERE receiver_id = %s",
            "SELECT MAX(date_time_received) FROM sent_documents WHERE issuer_id = %s"
        ]
        
        try:
            with self.conn.cursor() as cur:
                for query in queries:
                    cur.execute(query, (client_id,))
                    result = cur.fetchone()[0]
                    if result and (latest_ts is None or result > latest_ts):
                        latest_ts = result
            return latest_ts
        except psycopg2.Error as e:
            print(f"Failed to get latest invoice timestamp for {client_id}: {e}")
            return None