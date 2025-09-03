# single_client_sync_worker.py
from threading import Thread
import datetime
import pytz
from api_client import ETAApiClient
from db_manager import DatabaseManager

class SingleClientSyncWorker(Thread):
    def __init__(self, client_name, client_config, progress_queue):
        super().__init__()
        self.client_name = client_name
        self.client_config = client_config
        self.progress_queue = progress_queue
        self._is_running = True

    def stop(self):
        self._is_running = False

    def run(self):
        """The main sync logic for ONE client, now with a two-phase process."""
        client_name = self.client_name
        client_config = self.client_config
        cairo_tz = pytz.timezone('Africa/Cairo')
        now_in_cairo = datetime.datetime.now(cairo_tz)

        self.progress_queue.put(("LOG", f"--- Starting sync thread for: {client_name} ---"))
        db_params = { 'host': client_config.get('db_host'), 'dbname': client_config.get('db_name'), 'user': client_config.get('db_user'), 'password': client_config.get('db_pass'), 'port': int(client_config.get('db_port', 5432)) }
        api_client = ETAApiClient(client_config['client_id'], client_config['client_secret'])
        db_manager = DatabaseManager(db_params)
        
        if not db_manager.connect():
            self.progress_queue.put(("LOG", f"DB connection failed for {client_name}. Thread stopping."))
            self.progress_queue.put(("LIVE_UPDATE", (client_name, "DB Conn Fail")))
            return

        # --- NEW: PHASE 0 - Process Failed UUID Retry Queue ---
        self.progress_queue.put(("LOG", f"  -> Phase 0 ({client_name}): Checking for documents in retry queue..."))
        uuids_to_retry = client_config.get('failed_uuids', [])
        successfully_processed_retries = set()
        
        if uuids_to_retry:
            self.progress_queue.put(("LOG", f"    -> Found {len(uuids_to_retry)} documents to retry for {client_name}."))
            for uuid in uuids_to_retry:
                if not self._is_running: break
                details = api_client.get_document_details(uuid)
                if details:
                    is_sent = details.get('issuer', {}).get('id') == api_client.client_id
                    table_prefix = "sent_" if is_sent else ""
                    if not db_manager.document_exists(uuid, table_prefix):
                        db_manager.insert_document(details, table_prefix)
                    successfully_processed_retries.add(uuid)
                    self.progress_queue.put(("LOG", f"      -> SUCCESS on retried doc {uuid[:8]}."))
                else:
                    newly_failed_uuids_this_run.add(uuid)
                    self.progress_queue.put(("LOG", f"      -> FAILED again on retried doc {uuid[:8]}. Keeping in queue."))
        else:
            self.progress_queue.put(("LOG", f"    -> Retry queue is empty."))

        # --- PHASE 1: Re-check status of in-flux documents ---
        self.progress_queue.put(("LOG", f"  -> Phase 1 ({client_name}): Checking for status updates on recent documents..."))
        docs_to_recheck = db_manager.get_influx_document_uuids()
        updated_count = 0
        if docs_to_recheck:
            self.progress_queue.put(("LOG", f"    -> ({client_name}) Found {len(docs_to_recheck)} documents to re-validate."))
            for uuid in docs_to_recheck:
                if not self._is_running: break
                details = api_client.get_document_details(uuid)
                if details and details.get('status') != 'Valid':
                    # Determine table prefix based on the document's issuer/receiver ID
                    is_sent = details.get('issuer', {}).get('id') == api_client.client_id
                    table_prefix = "sent_" if is_sent else ""
                    
                    new_status = details.get('status')
                    reason = details.get('documentStatusReason', '')
                    db_manager.update_document_status(uuid, new_status, reason, table_prefix)
                    updated_count += 1
                    self.progress_queue.put(("LOG", f"      -> STATUS UPDATE ({client_name}): Doc {uuid[:8]} changed to '{new_status}'."))
        
        if updated_count > 0:
            self.progress_queue.put(("LOG", f"  -> Phase 1 Complete ({client_name}): Updated {updated_count} document statuses."))
        else:
            self.progress_queue.put(("LOG", f"  -> Phase 1 Complete ({client_name}): All recent document statuses are up-to-date."))
        
        if not self._is_running: # Allow cancellation after Phase 1
             db_manager.disconnect()
             return

        # --- PHASE 2: Discover new documents ---
        self.progress_queue.put(("LOG", f"  -> Phase 2 ({client_name}): Discovering new documents..."))
        start_date = db_manager.get_latest_invoice_timestamp()
        if start_date:
            start_date = start_date.astimezone(cairo_tz)
        else:
            start_date_str = client_config.get('oldest_invoice_date')
            if start_date_str: start_date = cairo_tz.localize(datetime.datetime.strptime(start_date_str, '%Y-%m-%d'))
            else: start_date = now_in_cairo - datetime.timedelta(days=30)
        
        total_new_docs = 0
        newest_doc_in_run = {'timestamp': None, 'uuid': None, 'internal_id': None}
        current_local_date = start_date.date()
        newly_failed_uuids_this_run = set()
        # --- THIS IS THE FULL, CORRECT DAY-BY-DAY LOOP THAT WAS MISSING ---
        while current_local_date <= now_in_cairo.date() and self._is_running:
            self.progress_queue.put(("LOG", f"  -> Processing Day: {current_local_date.strftime('%Y-%m-%d')} for {client_name}"))
            day_start_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.min))
            day_end_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.max))
            directions_to_sync = [("Received", ""), ("Sent", "sent_")]
            
            for direction, table_prefix in directions_to_sync:
                if not self._is_running: break
                continuation_token = None
                while True:
                    if not self._is_running: break
                    search_result = api_client.search_documents(day_start_local, day_end_local, continuation_token=continuation_token, direction=direction)
                    if search_result is None: break
                    
                    found_summaries = search_result.get('result', [])
                    if found_summaries:
                        self.progress_queue.put(("LOG", f"    -> Found {len(found_summaries)} '{direction}' documents for {client_name}."))

                    for summary in found_summaries:
                        if not self._is_running: break
                        uuid = summary['uuid']
                        if db_manager.document_exists(uuid, table_prefix): continue

                        details = api_client.get_document_details(uuid)
                        if details:
                            success, message = db_manager.insert_document(details, table_prefix)
                            if success:
                                total_new_docs += 1
        
                                doc_ts_str = details.get('dateTimeReceived') or details.get('dateTimeRecevied')
                                if doc_ts_str:
                                    if '.' in doc_ts_str and len(doc_ts_str.split('.')[1]) > 7: doc_ts_str = doc_ts_str[:26] + "Z"
                                    doc_dt = None
                                    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                                        try: doc_dt = datetime.datetime.strptime(doc_ts_str, fmt); break
                                        except ValueError: continue
                                    if doc_dt and (newest_doc_in_run['timestamp'] is None or doc_dt > newest_doc_in_run['timestamp']):
                                        newest_doc_in_run['timestamp'] = doc_dt
                                        newest_doc_in_run['uuid'] = details.get('uuid')
                                        newest_doc_in_run['internal_id'] = details.get('internalID') or details.get('document', {}).get('internalId')
                            else:
                                self.progress_queue.put(("LOG", f"DB_FAIL on doc {uuid[:8]}: {message}"))
                    
                    continuation_token = search_result.get('metadata', {}).get('continuationToken')
                    if continuation_token == "EndofResultSet" or not continuation_token: break
            
            if not self._is_running: break
            current_local_date += datetime.timedelta(days=1)

        if self._is_running:
            # First, update the sync status with the newest document found, if any.
            if total_new_docs > 0 and newest_doc_in_run['timestamp']:
                db_manager.update_sync_status(
                    client_config['client_id'], 
                    newest_doc_in_run['timestamp'], 
                    newest_doc_in_run['uuid'], 
                    newest_doc_in_run['internal_id']
                )
                display_time = newest_doc_in_run['timestamp'].astimezone(cairo_tz).strftime('%Y-%m-%d %H:%M (EET)')
                self.progress_queue.put(("LIVE_UPDATE", (client_name, f"Done ({display_time})")))
            else:
                self.progress_queue.put(("LIVE_UPDATE", (client_name, "Up to date")))

            # --- NEW: Now, save the updated failed UUIDs list ---
            final_failed_uuids_list = sorted(list((set(uuids_to_retry) - successfully_processed_retries) | newly_failed_uuids_this_run))
            config_manager.save_client_config(
                client_name, client_config.get('client_id'), client_config.get('client_secret'),
                client_config.get('db_host'), client_config.get('db_port'), client_config.get('db_name'),
                client_config.get('db_user'), client_config.get('db_pass'), client_config.get('date_span'),
                client_config.get('oldest_invoice_date'), client_config.get('skipped_days'), final_failed_uuids_list
            )
            self.progress_queue.put(("LOG", f"--- Finished sync thread for {client_name}. Found {total_new_docs} new documents. {len(final_failed_uuids_list)} documents remain in retry queue. ---"))
        
        db_manager.disconnect()