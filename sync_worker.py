# sync_worker.py
from threading import Thread
import datetime
import pytz

class SyncWorker(Thread):
    def __init__(self, client_id, api_client, db_manager, start_date, end_date, progress_queue):
        super().__init__()
        self.client_id = client_id
        self.api_client = api_client
        self.db_manager = db_manager
        self.start_date = start_date
        self.end_date = end_date
        self.progress_queue = progress_queue
        self._is_running = True

    def stop(self):
        self._is_running = False

    def run(self):
        cairo_tz = pytz.timezone('Africa/Cairo')
        
        current_local_date = self.start_date
        total_days = (self.end_date - self.start_date).days + 1
        processed_days = 0

        newest_doc_in_run = {'timestamp': None, 'uuid': None, 'internal_id': None}
        while current_local_date <= self.end_date and self._is_running:
            day_start_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.min))
            day_end_local = cairo_tz.localize(datetime.datetime.combine(current_local_date, datetime.time.max))

            self.progress_queue.put(("LOG", f"Processing Day: {current_local_date.strftime('%Y-%m-%d')}..."))

            # --- NEW: Two-pass logic for Sent and Received ---
            directions_to_sync = [("Received", ""), ("Sent", "sent_")]
            
            for direction, table_prefix in directions_to_sync:
                if not self._is_running: break
                
                try:
                    continuation_token = None
                    all_summaries = []
                    while True:
                        # Pass the direction to the API client
                        search_result = self.api_client.search_documents(day_start_local, day_end_local, continuation_token=continuation_token, direction=direction)
                        
                        if search_result is None:
                            self.progress_queue.put(("LOG", f"FATAL: Could not fetch {direction} summaries for {current_local_date.strftime('%Y-%m-%d')}."))
                            break
                        
                        all_summaries.extend(search_result.get('result', []))
                        continuation_token = search_result.get('metadata', {}).get('continuationToken')
                        if continuation_token == "EndofResultSet" or not continuation_token:
                            break

                    if all_summaries:
                        self.progress_queue.put(("LOG", f"  -> Found {len(all_summaries)} '{direction}' documents. Fetching details..."))

                    for i, summary in enumerate(all_summaries):
                        if not self._is_running: break
                        uuid = summary['uuid']
                        
                        # The table_prefix is now determined by the loop, not a guess
                        if self.db_manager.document_exists(uuid, table_prefix):
                            continue

                        details = self.api_client.get_document_details(uuid)
                        if details:
                            success, message = self.db_manager.insert_document(details, table_prefix)
                            if success:
                                self.progress_queue.put(("LOG", f"    -> Saved doc {i+1}/{len(all_summaries)} (UUID: {uuid[:8]}...)"))
                                
                                # --- NEW: Track the newest document found in this run ---
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
                                    
                                    if doc_dt and (newest_doc_in_run['timestamp'] is None or doc_dt > newest_doc_in_run['timestamp']):
                                        newest_doc_in_run['timestamp'] = doc_dt
                                        newest_doc_in_run['uuid'] = details.get('uuid')
                                        newest_doc_in_run['internal_id'] = details.get('internalID') or details.get('document', {}).get('internalId')
                            else:
                                self.progress_queue.put(("LOG", f"DB_FAIL on doc {uuid[:8]}: {message}"))
                
                except Exception as e:
                    self.progress_queue.put(("LOG", f"CRITICAL ERROR on {current_local_date.strftime('%Y-%m-%d')} for {direction} docs: {e}"))

            processed_days += 1
            progress_percent = (processed_days / total_days) if total_days > 0 else 1
            self.progress_queue.put(("PROGRESS", progress_percent))
            current_local_date += datetime.timedelta(days=1)
        
        if self._is_running and newest_doc_in_run['timestamp']:
            # Use the detailed info we tracked during the run
            self.db_manager.update_sync_status(
                self.api_client.client_id, 
                newest_doc_in_run['timestamp'], 
                newest_doc_in_run['uuid'], 
                newest_doc_in_run['internal_id']
            )

        self.progress_queue.put(("LOG", "Sync Finished!"))