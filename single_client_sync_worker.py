# single_client_sync_worker.py
from threading import Thread
import datetime
import pytz
from api_client import ETAApiClient
from db_manager import DatabaseManager
import config_manager # Import config_manager to save failed UUIDs

class SingleClientSyncWorker(Thread):
    def __init__(self, client_name, client_config, progress_queue):
        super().__init__()
        self.client_name = client_name
        self.client_config = client_config
        self.progress_queue = progress_queue
        self._is_running = True
        self.newest_doc_in_run = {'timestamp': None, 'uuid': None, 'internal_id': None}
        self.failed_uuids_in_run = set()

    def stop(self):
        self._is_running = False

    def _process_batch(self, db_manager, api_client, uuids_to_process, table_prefix, batch_name=""):
        """Processes a list of UUIDs by fetching details and saving them in a single batch."""
        if not uuids_to_process:
            return 0

        total_to_process = len(uuids_to_process)
        self.progress_queue.put(("LOG", f"    -> {batch_name}: Found {total_to_process} new documents. Fetching and batching..."))
        saved_count = 0
        
        try:
            with db_manager.conn.cursor() as cur:
                for i, uuid in enumerate(uuids_to_process):
                    if not self._is_running: raise InterruptedError("Sync cancelled.")
                    
                    details = api_client.get_document_details(uuid)
                    if details:
                        success = db_manager.insert_document(cur, details, table_prefix)
                        if success:
                            saved_count += 1
                            self.progress_queue.put(("LOG", f"      -> Batched {batch_name} doc {i+1}/{total_to_process} (UUID: {uuid[:8]}...)"))
                            doc_ts_str = details.get('dateTimeReceived') or details.get('dateTimeRecevied')
                            if doc_ts_str:
                                if '.' in doc_ts_str and len(doc_ts_str.split('.')[1]) > 7: doc_ts_str = doc_ts_str[:26] + "Z"
                                doc_dt = None
                                for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                                    try: doc_dt = datetime.datetime.strptime(doc_ts_str, fmt); break
                                    except ValueError: continue
                                if doc_dt and (self.newest_doc_in_run['timestamp'] is None or doc_dt > self.newest_doc_in_run['timestamp']):
                                    self.newest_doc_in_run['timestamp'] = doc_dt
                                    self.newest_doc_in_run['uuid'] = details.get('uuid')
                                    self.newest_doc_in_run['internal_id'] = details.get('internalID') or details.get('document', {}).get('internalId')
                        else:
                            self.progress_queue.put(("LOG", f"DB_FAIL on doc {uuid[:8]}: {message}"))
                    else:
                        self.progress_queue.put(("LOG", f"API_FAIL on doc {uuid[:8]}: Adding to retry queue."))
                        self.failed_uuids_in_run.add(uuid)
            
            db_manager.conn.commit()
            self.progress_queue.put(("LOG", f"    -> Batch of {saved_count} new '{batch_name}' documents committed."))
            return saved_count
            
        except InterruptedError:
            self.progress_queue.put(("LOG", f"  -> Batch cancelled for {batch_name}. Rolling back changes."))
            db_manager.conn.rollback()
        except Exception as e:
            self.progress_queue.put(("LOG", f"  -> CRITICAL BATCH ERROR for {batch_name}: {e}. Rolling back changes."))
            db_manager.conn.rollback()
        return 0

    def run(self):
        client_name = self.client_name
        client_config = self.client_config
        cairo_tz = pytz.timezone('Africa/Cairo')
        now_in_cairo = datetime.datetime.now(cairo_tz)

        self.progress_queue.put(("LOG", f"--- Starting sync thread for: {client_name} ---"))
        db_params = { 'host': client_config.get('db_host'), 'dbname': client_config.get('db_name'), 'user': client_config.get('db_user'), 'password': client_config.get('db_pass'), 'port': int(client_config.get('db_port', 5432)) }
        api_client = ETAApiClient(client_config.get('client_id'), client_config.get('client_secret'))
        db_manager = DatabaseManager(db_params)
        
        if not db_manager.connect():
            self.progress_queue.put(("LOG", f"DB connection failed for {client_name}. Thread stopping."))
            self.progress_queue.put(("LIVE_UPDATE", (client_name, "DB Conn Fail")))
            return

        # --- PHASE 0: Process Failed UUID Retry Queue (Robust Version) ---
        self.progress_queue.put(("LOG", f"  -> Phase 0 ({client_name}): Checking retry queue..."))
        uuids_to_retry = client_config.get('failed_uuids', [])
        successfully_processed_retries = set()
        
        if uuids_to_retry:
            self.progress_queue.put(("LOG", f"    -> Found {len(uuids_to_retry)} documents to retry for {client_name}."))
            
            # Create separate lists for sent and received to batch them correctly
            docs_to_insert_received = []
            docs_to_insert_sent = []

            for uuid in uuids_to_retry:
                if not self._is_running: break

                # First, check if the document has been saved since it last failed.
                # This prevents errors if a previous, interrupted run managed to save it.
                if db_manager.document_exists(uuid, "") or db_manager.document_exists(uuid, "sent_"):
                    successfully_processed_retries.add(uuid)
                    self.progress_queue.put(("LOG", f"      -> Doc {uuid[:8]} from retry queue already exists. Removing."))
                    continue

                details = api_client.get_document_details(uuid)
                if details:
                    # Now we have the details, we can determine the correct direction
                    is_sent = details.get('issuer', {}).get('id') == api_client.client_id
                    if is_sent:
                        docs_to_insert_sent.append(details)
                    else:
                        docs_to_insert_received.append(details)
                    
                    # Mark as successful for now, it will be removed from the list later
                    successfully_processed_retries.add(uuid)
                else:
                    self.progress_queue.put(("LOG", f"      -> FAILED again on retried doc {uuid[:8]}. Keeping in queue."))

            # Now, process the collected documents in their respective batches
            if docs_to_insert_received:
                self._process_batch_from_details(db_manager, docs_to_insert_received, "", "Retry-Received")
            
            if docs_to_insert_sent:
                self._process_batch_from_details(db_manager, docs_to_insert_sent, "sent_", "Retry-Sent")

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

        # PHASE 2: New Document Discovery
        self.progress_queue.put(("LOG", f"  -> Phase 2 ({client_name}): Discovering new documents..."))
        start_date = db_manager.get_latest_invoice_timestamp()
        if start_date: start_date = start_date.astimezone(cairo_tz)
        else:
            start_date_str = client_config.get('oldest_invoice_date')
            if start_date_str: start_date = cairo_tz.localize(datetime.datetime.strptime(start_date_str, '%Y-%m-%d'))
            else: start_date = now_in_cairo - datetime.timedelta(days=30)
        
        total_new_docs_in_phase2 = 0
        current_local_date = start_date.date()

        while current_local_date <= now_in_cairo.date() and self._is_running:
            self.progress_queue.put(("LOG", f"  -> Processing Day: {current_local_date.strftime('%Y-%m-%d')} for {client_name}"))
            day_start_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.min))
            day_end_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.max))
            directions_to_sync = [("Received", ""), ("Sent", "sent_")]
            
            for direction, table_prefix in directions_to_sync:
                if not self._is_running: break
                
                all_summaries = []
                continuation_token = None
                while True:
                    search_result = api_client.search_documents(day_start_local, day_end_local, continuation_token=continuation_token, direction=direction)
                    if search_result is None: break
                    all_summaries.extend(search_result.get('result', []))
                    continuation_token = search_result.get('metadata', {}).get('continuationToken')
                    if continuation_token == "EndofResultSet" or not continuation_token: break
                
                all_discovered_uuids = [s['uuid'] for s in all_summaries if isinstance(s, dict) and 'uuid' in s]
                if not all_discovered_uuids: continue

                uuids_to_process = db_manager.filter_existing_uuids(all_discovered_uuids, table_prefix)
                total_new_docs_in_phase2 += self._process_batch(db_manager, api_client, uuids_to_process, table_prefix, direction)

            if not self._is_running: break
            current_local_date += datetime.timedelta(days=1)

        # --- FINALIZATION ---
        if self._is_running:
            if total_new_docs_in_phase2 > 0 and self.newest_doc_in_run['timestamp']:
                db_manager.update_sync_status(client_config['client_id'], self.newest_doc_in_run['timestamp'], self.newest_doc_in_run['uuid'], self.newest_doc_in_run['internal_id'])
                display_time = self.newest_doc_in_run['timestamp'].astimezone(cairo_tz).strftime('%Y-%m-%d %H:%M (EET)')
                self.progress_queue.put(("LIVE_UPDATE", (client_name, f"Done ({display_time})")))
            else:
                self.progress_queue.put(("LIVE_UPDATE", (client_name, "Up to date")))

            final_failed_uuids_list = sorted(list((set(uuids_to_retry) - successfully_processed_retries) | self.failed_uuids_in_run))
            config_manager.save_client_config(
                client_name, client_config.get('client_id'), client_config.get('client_secret'),
                client_config.get('db_host'), client_config.get('db_port'), client_config.get('db_name'),
                client_config.get('db_user'), client_config.get('db_pass'), client_config.get('date_span'),
                client_config.get('oldest_invoice_date'), client_config.get('skipped_days'), final_failed_uuids_list
            )
            self.progress_queue.put(("LOG", f"--- Finished sync thread for {client_name}. Found {total_new_docs_in_phase2} new documents. {len(final_failed_uuids_list)} documents remain in retry queue. ---"))
        
        db_manager.disconnect()