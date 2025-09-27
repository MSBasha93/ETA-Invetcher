# sync_worker.py
from threading import Thread
import datetime
import pytz

class SyncWorker(Thread):
    def __init__(self, client_name, client_id, api_client, db_manager, start_date, end_date, progress_queue):
        super().__init__()
        self.client_id = client_id
        self.client_name = client_name
        self.api_client = api_client
        self.db_manager = db_manager
        self.start_date = start_date
        self.end_date = end_date
        self.progress_queue = progress_queue
        self._is_running = True
        self.newest_doc_in_run = {'timestamp': None, 'uuid': None, 'internal_id': None}
        self.skipped_days_in_run = []
        self.failed_uuids_in_run = set()

    def stop(self):
        self._is_running = False

    def run(self):
        cairo_tz = pytz.timezone('Africa/Cairo')
        
        current_local_date = self.end_date 
        total_days = (self.end_date - self.start_date).days + 1
        processed_days = 0

        while current_local_date >= self.start_date and self._is_running:
            day_start_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.min))
            day_end_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.max))
            self.progress_queue.put(("LOG", f"Processing Day: {current_local_date.strftime('%Y-%m-%d')}..."))
            
            day_had_api_failure = False
            directions_to_sync = [("Received", ""), ("Sent", "sent_")]
            
            for direction, table_prefix in directions_to_sync:
                if not self._is_running: break
                
                try:
                    # --- Step 1: Discover ALL document summaries for the day/direction ---
                    all_summaries = []
                    continuation_token = None
                    while True:
                        if not self._is_running: break
                        search_result = self.api_client.search_documents(day_start_local, day_end_local, continuation_token=continuation_token, direction=direction)
                        
                        if search_result is None:
                            day_had_api_failure = True; break
                        
                        all_summaries.extend(search_result.get('result', []))
                        continuation_token = search_result.get('metadata', {}).get('continuationToken')
                        if continuation_token == "EndofResultSet" or not continuation_token: break
                    
                    if day_had_api_failure: continue # Move to the next direction if discovery failed

                    # --- Step 2: Pre-filter against the database ---
                    all_discovered_uuids = [s['uuid'] for s in all_summaries if isinstance(s, dict) and 'uuid' in s]
                    uuids_to_process = self.db_manager.filter_existing_uuids(all_discovered_uuids, table_prefix)
                    
                    if not uuids_to_process:
                        if all_discovered_uuids: self.progress_queue.put(("LOG", f"  -> All {len(all_discovered_uuids)} discovered '{direction}' documents already exist."))
                        continue # Nothing to do, move to the next direction

                    summaries_to_process = {s['uuid']: s for s in all_summaries if s['uuid'] in uuids_to_process}
                    total_to_process = len(uuids_to_process)
                    self.progress_queue.put(("LOG", f"  -> Discovered {len(all_discovered_uuids)} '{direction}' documents, {total_to_process} are new. Fetching and batching..."))
                    
                    # --- Step 3: Process the new documents in a single batch ---
                    with self.db_manager.conn.cursor() as cur:
                        for i, uuid in enumerate(uuids_to_process):
                            if not self._is_running: raise InterruptedError("Sync cancelled.")
                            
                            details = self.api_client.get_document_details(uuid)
                            if details:
                                success = self.db_manager.insert_document(cur, details, table_prefix)
                                if success:
                                    self.progress_queue.put(("LOG", f"    -> Batched doc {i+1}/{total_to_process} (UUID: {uuid[:8]}...)"))
                                    # (The logic to track the newest doc is the same)
                                    doc_ts_str = details.get('dateTimeReceived') or details.get('dateTimeRecevied')
                                    if doc_ts_str:
                                        if '.' in doc_ts_str and len(doc_ts_str.split('.')[1]) > 7:
                                            doc_ts_str = doc_ts_str[:26] + "Z"
                                        doc_dt = None
                                        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                                            try:
                                                doc_dt = datetime.datetime.strptime(doc_ts_str, fmt)
                                                break
                                            except ValueError: continue
                                    
                                        if doc_dt and (self.newest_doc_in_run['timestamp'] is None or doc_dt > self.newest_doc_in_run['timestamp']):
                                            self.newest_doc_in_run['timestamp'] = doc_dt
                                            self.newest_doc_in_run['uuid'] = details.get('uuid')
                                            self.newest_doc_in_run['internal_id'] = details.get('internalID') or details.get('document', {}).get('internalId')
                                        pass
                                else:
                                    self.progress_queue.put(("LOG", f"DB_FAIL on doc {uuid[:8]}: Skipping doc in batch."))
                            else:
                                self.progress_queue.put(("LOG", f"API_FAIL on doc {uuid[:8]}: Adding to retry queue."))
                                self.failed_uuids_in_run.add(uuid)
                    
                    self.db_manager.conn.commit()
                    self.progress_queue.put(("LOG", f"  -> Batch of {total_to_process} new '{direction}' documents committed."))

                except InterruptedError:
                    self.progress_queue.put(("LOG", "  -> Batch cancelled. Rolling back changes."))
                    self.db_manager.conn.rollback()
                except Exception as e:
                    self.progress_queue.put(("LOG", f"CRITICAL ERROR on {current_local_date.strftime('%Y-%m-%d')} for {direction} docs: {e}"))
                    self.db_manager.conn.rollback()
                    day_had_api_failure = True
            
            if day_had_api_failure:
                self.skipped_days_in_run.append(current_local_date.strftime('%Y-%m-%d'))

            processed_days += 1
            progress_percent = (processed_days / total_days) if total_days > 0 else 1
            self.progress_queue.put(("PROGRESS", progress_percent))
            current_local_date -= datetime.timedelta(days=1)
        
        if self._is_running and self.newest_doc_in_run['timestamp']:
            self.db_manager.update_sync_status(
                self.api_client.client_id, 
                self.newest_doc_in_run['timestamp'], 
                self.newest_doc_in_run['uuid'], 
                self.newest_doc_in_run['internal_id']
            )
        
        self.progress_queue.put(("HISTORICAL_SYNC_COMPLETE", (self.skipped_days_in_run, list(self.failed_uuids_in_run), self.client_name)))