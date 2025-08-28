# live_sync_manager.py
from threading import Thread
from single_client_sync_worker import SingleClientSyncWorker # Import the new worker

class LiveSyncManager(Thread):
    def __init__(self, all_clients_data, progress_queue):
        super().__init__()
        self.selected_clients = all_clients_data
        self.progress_queue = progress_queue
        self.worker_threads = []
        self._is_running = True

    def stop(self):
        """Tells all running child worker threads to stop."""
        self._is_running = False
        self.progress_queue.put(("LOG", "--- Cancellation signal sent to all live sync threads ---"))
        for worker in self.worker_threads:
            worker.stop()

    def run(self):
        self.progress_queue.put(("LOG", "--- Live Sync Manager Started: Spawning parallel workers ---"))

        # Create and start a worker thread for each selected client
        for client_name, client_config in self.selected_clients.items():
            if not self._is_running: break
            
            worker = SingleClientSyncWorker(client_name, client_config, self.progress_queue)
            worker.daemon = True # Ensure they die if the main app closes
            self.worker_threads.append(worker)
            worker.start()

        # Wait for all worker threads to complete their execution
        for worker in self.worker_threads:
            worker.join()

        # Only send completion message if it wasn't cancelled
        if self._is_running:
            self.progress_queue.put(("LOG", "--- All parallel sync threads have finished. ---"))
            self.progress_queue.put(("LIVE_SYNC_COMPLETE", None))