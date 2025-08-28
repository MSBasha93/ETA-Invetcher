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

    def _ensure_connection(self):
        """Checks if the connection is open and reconnects if it's not."""
        if self.conn is None or self.conn.closed != 0:
            print("Database connection is closed. Reconnecting...")
            self.connect()

    def check_and_create_tables(self):
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
        self._ensure_connection()
        table_name = f"{table_prefix}documents"; query = f"SELECT 1 FROM {table_name} WHERE uuid = %s"
        with self.conn.cursor() as cur: cur.execute(query, (uuid,)); return cur.fetchone() is not None
    
    def insert_document(self, doc_data, table_prefix=""):
        self._ensure_connection()
        header_table = f"{table_prefix}documents"
        lines_table = f"{table_prefix}document_lines"
        
        try:
            with self.conn.cursor() as cur:
                doc_header = doc_data.get('document', doc_data)

                # Prepare header data with safe access using .get()
                header_data = {
                    "uuid": doc_data.get('uuid'), "submission_uuid": doc_data.get('submissionUUID'),
                    "internal_id": doc_header.get('internalID'), "type_name": doc_header.get('documentType'),
                    "document_type_version": doc_header.get('documentTypeVersion'),
                    # Issuer Info
                    "issuer_id": doc_header.get('issuer', {}).get('id'), "issuer_name": doc_header.get('issuer', {}).get('name'),
                    "issuer_type": str(doc_header.get('issuer', {}).get('type')),
                    "issuer_address_street": doc_header.get('issuer', {}).get('address', {}).get('street'),
                    "issuer_address_building_number": doc_header.get('issuer', {}).get('address', {}).get('buildingNumber'),
                    "issuer_address_governate": doc_header.get('issuer', {}).get('address', {}).get('governate'),
                    # Receiver Info
                    "receiver_id": doc_header.get('receiver', {}).get('id'), "receiver_name": doc_header.get('receiver', {}).get('name'),
                    "receiver_type": str(doc_header.get('receiver', {}).get('type')),
                    "receiver_address_street": doc_header.get('receiver', {}).get('address', {}).get('street'),
                    "receiver_address_building_number": doc_header.get('receiver', {}).get('address', {}).get('buildingNumber'),
                    "receiver_address_governate": doc_header.get('receiver', {}).get('address', {}).get('governate'),
                    # Dates
                    "date_time_issued": doc_header.get('dateTimeIssued'),
                    "date_time_received": doc_data.get('dateTimeReceived') or doc_data.get('dateTimeRecevied'),
                    # Status
                    "status": doc_data.get('status'), "document_status_reason": doc_data.get('documentStatusReason'),
                    # Totals
                    "total_amount": doc_data.get('totalAmount'), "net_amount": doc_data.get('netAmount'),
                    "total_sales": doc_data.get('totalSales'), "total_discount": doc_data.get('totalDiscount'),
                    "total_items_discount_amount": doc_data.get('totalItemsDiscountAmount'), "extra_discount_amount": doc_data.get('extraDiscountAmount'),
                    # References
                    "sales_order_reference": doc_data.get('salesOrderReference'), "purchase_order_reference": doc_data.get('purchaseOrderReference')
                }

                # Dynamically populate tax fields
                for i, tax_total in enumerate(doc_data.get('taxTotals', [])):
                    if i >= 5: break # Don't exceed 5 tax fields
                    header_data[f'tax{i+1}_type'] = tax_total.get('taxType')
                    header_data[f'tax{i+1}_amount'] = tax_total.get('amount')

                # Build the SQL statement with only the keys we have
                columns = ', '.join(header_data.keys())
                placeholders = ', '.join([f'%({key})s' for key in header_data.keys()])
                header_sql = f"INSERT INTO {header_table} ({columns}) VALUES ({placeholders});"
                cur.execute(header_sql, header_data)

                # Process invoice lines
                for line in doc_data.get('invoiceLines', []):
                    line_data = {
                        "document_uuid": doc_data.get('uuid'), "description": line.get('description'),
                        "item_type": line.get('itemType'), "item_code": line.get('itemCode'),
                        "internal_code": line.get('internalCode'), "quantity": line.get('quantity'),
                        "unit_type": line.get('unitType'),
                        "unit_value_amount_egp": line.get('unitValue', {}).get('amountEGP'),
                        "sales_total": line.get('salesTotal'), "net_total": line.get('netTotal'),
                        "total": line.get('total'),
                        "discount_rate": line.get('discount', {}).get('rate'),
                        "discount_amount": line.get('discount', {}).get('amount')
                    }
                    # Dynamically populate line tax fields
                    for i, tax_item in enumerate(line.get('lineTaxableItems', [])):
                        if i >= 5: break
                        line_data[f'tax{i+1}_type'] = tax_item.get('taxType')
                        line_data[f'tax{i+1}_amount'] = tax_item.get('amount')
                    
                    columns = ', '.join(line_data.keys())
                    placeholders = ', '.join([f'%({key})s' for key in line_data.keys()])
                    lines_sql = f"INSERT INTO {lines_table} ({columns}) VALUES ({placeholders});"
                    cur.execute(lines_sql, line_data)
                
            self.conn.commit()
            return (True, "Success")
        except (psycopg2.Error, ValueError) as e:
            error_message = f"DB Error on doc {doc_data.get('uuid')}: {e}"
            print(error_message)
            self.conn.rollback()
            return (False, str(e).strip())

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
            conn = psycopg2.connect(**temp_params)
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