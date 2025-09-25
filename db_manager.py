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
            self.conn = self.conn = psycopg2.connect(**self.db_params, sslmode='require')
            return True
        except psycopg2.OperationalError:
            return False

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def _ensure_connection(self):
        """Checks if the connection is open and reconnects if it's not."""
        if self.conn is None or self.conn.closed != 0:
            print("Database connection is closed. Reconnecting...")
            self.connect()

    def check_and_create_tables(self):
        self._ensure_connection()
        """
        Creates the FULL, detailed schema matching your 'menna' database.
        """
        commands = (
            # --- Documents and Sent_Documents (Identical, detailed schema) ---
            """
            CREATE TABLE IF NOT EXISTS documents (
                uuid VARCHAR(255) PRIMARY KEY, submission_uuid VARCHAR(255), long_id VARCHAR(255), internal_id VARCHAR(255), type_name VARCHAR(255),
                document_type_name_primary_lang VARCHAR(255), document_type_name_secondary_lang VARCHAR(255), type_version_name VARCHAR(255),
                document_type_version VARCHAR(255), document_type VARCHAR(255), issuer_id VARCHAR(255), issuer_name VARCHAR(255), issuer_type VARCHAR(255),
                issuer_address_branch_id VARCHAR(255), issuer_address_country VARCHAR(255), issuer_address_governate VARCHAR(255),
                issuer_address_region_city VARCHAR(255), issuer_address_street TEXT, issuer_address_building_number VARCHAR(255),
                issuer_address_floor VARCHAR(255), issuer_address_room VARCHAR(255), issuer_address_landmark VARCHAR(255), issuer_address_additional_information TEXT,
                receiver_id VARCHAR(255), receiver_name VARCHAR(255), receiver_type VARCHAR(255), receiver_address_branch_id VARCHAR(255),
                receiver_address_country VARCHAR(255), receiver_address_governate VARCHAR(255), receiver_address_region_city VARCHAR(255),
                receiver_address_street TEXT, receiver_address_building_number VARCHAR(255), receiver_address_floor VARCHAR(255),
                receiver_address_room VARCHAR(255), receiver_address_landmark VARCHAR(255), receiver_address_additional_information TEXT,
                date_time_issued TIMESTAMP, date_time_received TIMESTAMP, service_delivery_date TIMESTAMP, customs_clearance_date TIMESTAMP,
                validation_status VARCHAR(255), transformation_status VARCHAR(255), status_id INT, status VARCHAR(255), document_status_reason TEXT,
                cancel_request_date TIMESTAMP, reject_request_date TIMESTAMP, cancel_request_delayed_date TIMESTAMP, reject_request_delayed_date TIMESTAMP,
                decline_cancel_request_date TIMESTAMP, decline_reject_request_date TIMESTAMP, canbe_cancelled_until TIMESTAMP, canbe_rejected_until TIMESTAMP,
                submission_channel INT, freeze_status_frozen BOOLEAN, freeze_status_type VARCHAR(255), freeze_status_scope VARCHAR(255),
                freeze_status_action_date TIMESTAMP, freeze_status_au_code VARCHAR(255), freeze_status_au_name VARCHAR(255),
                customs_declaration_number VARCHAR(255), e_payment_number VARCHAR(255), public_url TEXT, purchase_order_description TEXT,
                sales_order_description TEXT, sales_order_reference VARCHAR(255), proforma_invoice_number VARCHAR(255), purchase_order_reference VARCHAR(255),
                late_submission_request_number VARCHAR(255), additional_metadata TEXT, alert_details TEXT, signatures TEXT, doc_references TEXT,
                total_items_discount_amount NUMERIC, total_amount NUMERIC, net_amount NUMERIC, total_discount NUMERIC, total_sales NUMERIC,
                extra_discount_amount NUMERIC, max_percision INT, document_lines_total_count INT,
                tax1_type VARCHAR(50), tax1_amount NUMERIC, tax2_type VARCHAR(50), tax2_amount NUMERIC,
                tax3_type VARCHAR(50), tax3_amount NUMERIC, tax4_type VARCHAR(50), tax4_amount NUMERIC,
                tax5_type VARCHAR(50), tax5_amount NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """ CREATE TABLE IF NOT EXISTS sent_documents (LIKE documents INCLUDING ALL); """, # Efficiently duplicates the table
            
            # --- Document_Lines and Sent_Document_Lines (Identical, detailed schema) ---
            """
            CREATE TABLE IF NOT EXISTS document_lines (
                id SERIAL PRIMARY KEY, custom_uuid VARCHAR(255), document_uuid VARCHAR(255), item_primary_name TEXT, item_primary_description TEXT,
                item_secondary_name TEXT, item_secondary_description TEXT, item_type VARCHAR(255), item_code VARCHAR(255),
                internal_code VARCHAR(255), description TEXT, unit_type VARCHAR(255), unit_type_primary_name TEXT,
                unit_type_primary_description TEXT, unit_type_secondary_name TEXT, unit_type_secondary_description TEXT,
                quantity NUMERIC, weight_unit_type VARCHAR(255), weight_unit_type_primary_name TEXT, weight_unit_type_primary_description TEXT,
                weight_unit_type_secondary_name TEXT, weight_unit_type_secondary_description TEXT, weight_quantity NUMERIC,
                unit_value_currency_sold VARCHAR(50), unit_value_amount_sold NUMERIC, unit_value_amount_egp NUMERIC,
                unit_value_currency_exchange_rate NUMERIC, factory_unit_value_currency_sold VARCHAR(50),
                factory_unit_value_amount_sold NUMERIC, factory_unit_value_amount_egp NUMERIC,
                factory_unit_value_currency_exchange_rate NUMERIC, sales_total NUMERIC, sales_total_foreign NUMERIC,
                net_total NUMERIC, net_total_foreign NUMERIC, total NUMERIC, total_foreign NUMERIC,
                items_discount NUMERIC, items_discount_foreign NUMERIC, total_taxable_fees NUMERIC,
                total_taxable_fees_foreign NUMERIC, value_difference NUMERIC, value_difference_foreign NUMERIC,
                discount_amount NUMERIC, discount_rate NUMERIC, discount_amount_foreign NUMERIC,
                tax1_type VARCHAR(50), tax1_amount NUMERIC, tax2_type VARCHAR(50), tax2_amount NUMERIC,
                tax3_type VARCHAR(50), tax3_amount NUMERIC, tax4_type VARCHAR(50), tax4_amount NUMERIC,
                tax5_type VARCHAR(50), tax5_amount NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """ CREATE TABLE IF NOT EXISTS sent_document_lines (LIKE document_lines INCLUDING ALL); """,
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
        view_commands = (
            """
            CREATE OR REPLACE VIEW vw_accounts_payable_line_items AS
            SELECT 
                line.custom_uuid AS line_item_uuid,
                line.document_uuid,
                hdr.internal_id AS document_internal_id,
                hdr.uuid AS header_uuid,
                hdr.date_time_issued::date AS issue_date,
                hdr.issuer_name AS supplier_name,
                hdr.document_type,
                hdr.type_name,
                hdr.status,
                line.item_primary_name AS item_name,
                line.description AS item_description,
                line.item_code,
                line.quantity,
                line.unit_value_amount_egp AS unit_price_egp,
                line.sales_total AS gross_amount_egp,
                line.items_discount AS discount_amount_egp,
                line.net_total AS net_amount_egp,
                (line.total - line.net_total) AS tax_amount_egp,
                line.total AS total_amount_egp,
                line.unit_value_currency_sold AS original_currency,
                line.total_foreign AS total_amount_foreign
            FROM 
                documents hdr
            JOIN 
                document_lines line ON hdr.uuid = line.document_uuid;
            """,
            """
            CREATE OR REPLACE VIEW vw_accounts_receivable_line_items AS
            SELECT 
                line.custom_uuid AS line_item_uuid,
                line.document_uuid,
                hdr.internal_id AS document_internal_id,
                hdr.uuid AS header_uuid,
                hdr.date_time_issued::date AS issue_date,
                hdr.receiver_name AS customer_name,
                hdr.document_type,
                hdr.type_name,
                hdr.status,
                line.item_primary_name AS item_name,
                line.description AS item_description,
                line.item_code,
                line.quantity,
                line.unit_value_amount_egp AS unit_price_egp,
                line.sales_total AS gross_amount_egp,
                line.items_discount AS discount_amount_egp,
                line.net_total AS net_amount_egp,
                (line.total - line.net_total) AS tax_amount_egp,
                line.total AS total_amount_egp,
                line.unit_value_currency_sold AS original_currency,
                line.total_foreign AS total_amount_foreign
            FROM 
                sent_documents hdr
            JOIN 
                sent_document_lines line ON hdr.uuid = line.document_uuid;
            """,
            """
            CREATE OR REPLACE VIEW vw_unified_financial_ledger AS
            WITH all_transactions AS (
                SELECT 
                    'Revenue' AS transaction_type,
                    line_item_uuid, document_uuid, document_internal_id, document_type,
                    issue_date, customer_name AS partner_name, item_name, item_description,
                    quantity, unit_price_egp, gross_amount_egp, discount_amount_egp,
                    net_amount_egp, tax_amount_egp, total_amount_egp,
                    original_currency, total_amount_foreign
                FROM 
                    vw_accounts_receivable_line_items
                UNION ALL
                SELECT 
                    'Expense' AS transaction_type,
                    line_item_uuid, document_uuid, document_internal_id, document_type,
                    issue_date, supplier_name AS partner_name, item_name, item_description,
                    quantity, unit_price_egp, gross_amount_egp, discount_amount_egp,
                    net_amount_egp, tax_amount_egp, total_amount_egp,
                    original_currency, total_amount_foreign
                FROM 
                    vw_accounts_payable_line_items
            )
            SELECT 
                transaction_type,
                CASE
                    WHEN document_type = 'I' THEN 'Invoice'
                    WHEN document_type = 'C' THEN 'Credit Note'
                    WHEN document_type = 'D' THEN 'Debit Note'
                    ELSE 'Other'
                END AS document_category,
                line_item_uuid, document_uuid, document_internal_id, issue_date,
                partner_name, item_name, item_description, quantity,
                unit_price_egp, gross_amount_egp, discount_amount_egp,
                net_amount_egp, tax_amount_egp, total_amount_egp,
                original_currency, total_amount_foreign
            FROM all_transactions t;
            """
        )
        try:
            with self.conn.cursor() as cur:
                # --- THIS IS THE CRITICAL FIX ---
                # We combine all commands into one list to execute them sequentially.
                all_commands = list(commands) + list(view_commands)
                
                for command in all_commands:
                    try:
                        # Each command is executed in its own transaction context within the loop
                        print(f"Executing schema command: {command[:70]}...") # Log what we're doing
                        cur.execute(command)
                        print("  -> Command successful.")
                    except psycopg2.Error as e:
                        print(f"  -> ERROR executing command: {e}")
                        print("     -> Skipping this command and continuing...")
                        self.conn.rollback() # Rollback the single failed command
                    else:
                        self.conn.commit() # Commit the single successful command

            print("Schema verification complete.")
            return (True, "Schema is ready.")

        except psycopg2.Error as e:
            print(f"A fatal error occurred during schema creation: {e}")
            self.conn.rollback()
            return (False, str(e).strip())

    def check_and_create_readonly_user(self):
        self._ensure_connection()
        """
        Checks if a read-only user for the current database exists, and creates it
        with the correct permissions if it doesn't.
        This requires the main connection user to have CREATEROLE privileges.
        """
        db_name = self.db_params.get('dbname')
        if not db_name:
            return (False, "Database name is not configured.")

        # Construct the username and password based on the database name
        ro_username = f"{db_name}_user"
        ro_password = f"{db_name}@FN"
        
        try:
            # --- THIS IS THE CRITICAL FIX ---
            # Temporarily set autocommit to True for user management commands
            self.conn.autocommit = True
            
            with self.conn.cursor() as cur:
                # Step 1: Check if the user/role already exists
                cur.execute("SELECT 1 FROM pg_catalog.pg_user WHERE usename = %s", (ro_username,))
                user_exists = cur.fetchone()

                if not user_exists:
                    # Step 2: Create the user only if they don't exist
                    print(f"User '{ro_username}' does not exist. Creating...")
                    # Use SQL parameters for the password to be safe
                    cur.execute(f'CREATE USER "{ro_username}" WITH PASSWORD %s;', (ro_password,))
                    print(f"  -> User '{ro_username}' created successfully.")
                else:
                    print(f"User '{ro_username}' already exists. Verifying permissions...")

                # Step 3 & 4: Grant necessary privileges
                cur.execute(f'GRANT CONNECT ON DATABASE "{db_name}" TO "{ro_username}";')
                cur.execute(f'GRANT USAGE ON SCHEMA public TO "{ro_username}";')
                cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{ro_username}";')
                # Step 5: Grant privileges for any future tables/views
                cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{ro_username}";')
                
            self.conn.commit()
            success_msg = f"Read-only user '{ro_username}' is configured."
            print(f"  -> {success_msg}")
            return (True, success_msg)

        except psycopg2.Error as e:
            error_message = str(e).strip()
            # Provide a helpful message for the most common failure
            if "permission denied" in error_message:
                error_message = "Permission denied. The main user needs CREATEROLE privileges to create other users."
            print(f"  -> ERROR during user creation/verification: {error_message}")
            self.conn.rollback()
            return (False, error_message)
        finally:
            # --- ALWAYS ensure we turn autocommit back off ---
            if self.conn:
                self.conn.autocommit = False

    def get_all_sync_statuses(self):
        self._ensure_connection()
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
        self._ensure_connection()
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

    def update_document_status(self, uuid, new_status, reason, table_prefix=""):
        self._ensure_connection()
        """Updates the status and reason for a single document."""
        table_name = f"{table_prefix}documents"
        sql = f"UPDATE {table_name} SET status = %s, document_status_reason = %s WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (new_status, reason, uuid))
            self.conn.commit()
            return True
        except psycopg2.Error as e:
            print(f"Failed to update status for doc {uuid}: {e}")
            self.conn.rollback()
            return False

    def document_exists(self, uuid, table_prefix=""):
        self._ensure_connection()
        table_name = f"{table_prefix}documents"; query = f"SELECT 1 FROM {table_name} WHERE uuid = %s"
        with self.conn.cursor() as cur: cur.execute(query, (uuid,)); return cur.fetchone() is not None

    def filter_existing_uuids(self, uuids_to_check, table_prefix=""):
        self._ensure_connection()
        """
        Takes a list of UUIDs and returns a new list containing only the UUIDs
        that do NOT already exist in the database.
        """
        if not uuids_to_check:
            return []
        
        table_name = f"{table_prefix}documents"
        # The '= ANY(%s)' syntax is a very efficient way to check for multiple values
        query = f"SELECT uuid FROM {table_name} WHERE uuid = ANY(%s);"
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (uuids_to_check,))
                # Create a set of the UUIDs that were found in the database
                existing_uuids = {row[0] for row in cur.fetchall()}
            
            # Return a new list containing only the UUIDs that were NOT in the existing set
            new_uuids = [uuid for uuid in uuids_to_check if uuid not in existing_uuids]
            return new_uuids

        except psycopg2.Error as e:
            print(f"Error filtering existing UUIDs: {e}")
            self.conn.rollback()
            # Failsafe: if the check fails, return the original list to avoid losing data
            return uuids_to_check
    

    def insert_document(self, cursor, doc_data, table_prefix=""):
        self._ensure_connection()
        """
        Inserts a single document using a provided database cursor for batching.
        This version is designed to be called from a worker's transaction loop.
        """
        header_table = f"{table_prefix}documents"
        lines_table = f"{table_prefix}document_lines"
        
        try:
            # The logic for parsing is the same as the last complete version.
            # It correctly handles all three JSON structures.
            core_data_object = doc_data.get('document', doc_data)

            header_data = {
                "uuid": doc_data.get('uuid'),
                "submission_uuid": doc_data.get('submissionUUID'),
                "status": doc_data.get('status'),
                "total_amount": doc_data.get('totalAmount'),
                "net_amount": doc_data.get('netAmount'),
                "total_sales": doc_data.get('totalSales'),
                "total_discount": doc_data.get('totalDiscount'),
                "date_time_received": doc_data.get('dateTimeReceived') or doc_data.get('dateTimeRecevied'),
                "document_status_reason": doc_data.get('documentStatusReason'),
                "internal_id": core_data_object.get('internalID') or core_data_object.get('internalId'),
                "type_name": core_data_object.get('documentType'),
                "date_time_issued": core_data_object.get('dateTimeIssued'),
                "issuer_id": core_data_object.get('issuer', {}).get('id'),
                "issuer_name": core_data_object.get('issuer', {}).get('name'),
                "receiver_id": core_data_object.get('receiver', {}).get('id'),
                "receiver_name": core_data_object.get('receiver', {}).get('name')
            }
            
            columns = ', '.join(header_data.keys())
            placeholders = ', '.join([f'%({key})s' for key in header_data.keys()])
            header_sql = f"INSERT INTO {header_table} ({columns}) VALUES ({placeholders});"
            # Use the provided cursor
            cursor.execute(header_sql, header_data)

            for line in core_data_object.get('invoiceLines', []):
                line_data = {
                    "document_uuid": doc_data.get('uuid'), "description": line.get('description'),
                    "item_code": line.get('itemCode'), "quantity": line.get('quantity'),
                    "net_total": line.get('netTotal'), "total": line.get('total')
                }
                line_columns = ', '.join(line_data.keys())
                line_placeholders = ', '.join([f'%({key})s' for key in line_data.keys()])
                lines_sql = f"INSERT INTO {lines_table} ({line_columns}) VALUES ({line_placeholders});"
                # Use the provided cursor
                cursor.execute(lines_sql, line_data)
            
            return True # Signal success to the calling worker

        except (psycopg2.Error, ValueError) as e:
            # We don't rollback here; the worker that owns the transaction will.
            print(f"DB Batch Error on doc {doc_data.get('uuid')}: {e}")
            return False # Signal failure

    def get_latest_invoice_timestamp(self):
        self._ensure_connection()
        query = """
            SELECT MAX(date_time_received) FROM (
                SELECT date_time_received FROM documents
                UNION ALL
                SELECT date_time_received FROM sent_documents
            ) AS all_dates;
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
                return result # This will be the latest timestamp or None
        except psycopg2.Error as e:
            print(f"Failed to get latest invoice timestamp: {e}")
            self.conn.rollback()
            return None

    def get_influx_document_uuids(self):
        self._ensure_connection()
        """
        Fetches the UUIDs of all 'Valid' documents whose cancellation/rejection
        period has not yet passed. These are the only documents that need re-checking.
        """
        uuids = []
        # We check both tables. The NOW() function is timezone-aware in PostgreSQL.
        queries = [
            "SELECT uuid FROM documents WHERE status = 'Valid' AND canbe_cancelled_until > NOW()",
            "SELECT uuid FROM sent_documents WHERE status = 'Valid' AND canbe_cancelled_until > NOW()"
        ]
        try:
            with self.conn.cursor() as cur:
                for query in queries:
                    cur.execute(query)
                    # fetchall() returns a list of tuples, e.g., [('uuid1',), ('uuid2',)]
                    results = cur.fetchall()
                    for row in results:
                        uuids.append(row[0])
            return uuids
        except psycopg2.Error as e:
            print(f"Failed to get in-flux document UUIDs: {e}")
            self.conn.rollback()
            return []

    def create_database(self, new_db_name):
        """
        Connects to the maintenance 'postgres' DB to create a new database.
        Returns (True, "Success message") or (False, "Error message").
        """
        # Create a temporary connection config, overriding the dbname
        # to connect to the default 'postgres' database.
        temp_params = self.db_params.copy()
        temp_params['dbname'] = 'postgres'
        
        conn = None
        try:
            # Establish the connection to the maintenance database
            conn = psycopg2.connect(**temp_params, sslmode='require')
            # CREATE DATABASE cannot run inside a transaction, so we use autocommit.
            conn.autocommit = True
            
            with conn.cursor() as cur:
                # We use double quotes to handle case-sensitivity and special chars safely.
                cur.execute(f'CREATE DATABASE "{new_db_name}";')
            
            return (True, f"Database '{new_db_name}' created successfully!")

        except psycopg2.errors.DuplicateDatabase:
            # This is the specific error for an existing DB. It's not a failure.
            return (False, f"Database '{new_db_name}' already exists.")
        except psycopg2.Error as e:
            # Catch other errors like "permission denied"
            print(f"Error creating database: {e}")
            return (False, str(e).strip())
        finally:
            if conn:
                conn.close()