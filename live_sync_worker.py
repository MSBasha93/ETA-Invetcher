# live_sync_worker.py
from threading import Thread
import datetime
import pytz
from api_client import ETAApiClient
from db_manager import DatabaseManager

class LiveSyncWorker(Thread):
    def __init__(self, all_clients_data, progress_queue):
        super().__init__()
        self.all_clients = all_clients_data
        self.progress_queue = progress_queue
        self._is_running = True

    def stop(self):
        self._is_running = False

    def run(self):
        self.progress_queue.put(("LOG", "--- Starting Live Sync for all clients ---"))
        cairo_tz = pytz.timezone('Africa/Cairo')
        now_in_cairo = datetime.datetime.now(cairo_tz)

        for client_name, client_config in self.all_clients.items():
            if not self._is_running:
                self.progress_queue.put(("LOG", "Live Sync cancelled by user."))
                return

            self.progress_queue.put(("LIVE_UPDATE", (client_name, "Processing...")))
            self.progress_queue.put(("LOG", f"--- Syncing client: {client_name} ---"))

            db_params = {
                'host': client_config.get('db_host'), 'dbname': client_config.get('db_name'),
                'user': client_config.get('db_user'), 'password': client_config.get('db_pass'),
                'port': client_config.get('db_port', 5432)
            }
            api_client = ETAApiClient(client_config['client_id'], client_config['client_secret'])
            db_manager = DatabaseManager(db_params)
            
            # --- CRITICAL FIX: Connect to DB *before* getting the start date ---
            if not db_manager.connect():
                self.progress_queue.put(("LOG", f"DB connection failed for {client_name}. Skipping."))
                self.progress_queue.put(("LIVE_UPDATE", (client_name, "DB Conn Fail")))
                continue

            # Now that we are connected, we can reliably get the latest timestamp
            start_date = db_manager.get_latest_invoice_timestamp()
            if start_date:
                start_date = start_date.astimezone(cairo_tz)
            else:
                start_date_str = client_config.get('oldest_invoice_date')
                if start_date_str:
                    start_date = cairo_tz.localize(datetime.datetime.strptime(start_date_str, '%Y-%m-%d'))
                else:
                    start_date = now_in_cairo - datetime.timedelta(days=30)
            
            self.progress_queue.put(("LOG", f"Syncing {client_name} from start of day {start_date.strftime('%Y-%m-%d')} until now."))
            
            total_new_docs = 0
            newest_doc_in_run = {'timestamp': None, 'uuid': None, 'internal_id': None}
            # --- LOGIC FIX: Always start from the beginning of the last synced day ---
            current_local_date = start_date.date()

            while current_local_date <= now_in_cairo.date() and self._is_running:
                # --- LOGGING FIX: Report the day being processed ---
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
                        # --- LOGGING FIX: Report when documents are found ---
                        if found_summaries:
                            self.progress_queue.put(("LOG", f"    -> Found {len(found_summaries)} '{direction}' documents."))

                        for summary in found_summaries:
                            if not self._is_running: break
                            uuid = summary['uuid']
                            
                            if db_manager.document_exists(uuid, table_prefix):
                                continue

                            details = api_client.get_document_details(uuid)
                            if details:
                                success, message = db_manager.insert_document(details, table_prefix)
                                if success:
                                    total_new_docs += 1
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
                        
                        continuation_token = search_result.get('metadata', {}).get('continuationToken')
                        if continuation_token == "EndofResultSet" or not continuation_token:
                            break
                
                if not self._is_running: break
                current_local_date += datetime.timedelta(days=1)


            if self._is_running:
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
                
                self.progress_queue.put(("LOG", f"Finished syncing {client_name}. Found {total_new_docs} new documents."))
            
            db_manager.disconnect()

        if self._is_running:
            self.progress_queue.put(("LOG", "--- Live Sync for all clients complete! ---"))