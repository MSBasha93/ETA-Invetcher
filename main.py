# main.py
import customtkinter as ctk
from tkcalendar import DateEntry
import datetime
import queue
import threading
import csv
import json
from tkinter import filedialog, messagebox
import config_manager
from api_client import ETAApiClient
from db_manager import DatabaseManager
from sync_worker import SyncWorker
from live_sync_worker import LiveSyncWorker # NEW IMPORT

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("ETA-Fetcher")
        self.geometry("900x750")

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        self.clients = {}
        self.selected_client_name = ctk.StringVar()
        self.api_client = None
        self.db_manager = None
        self.sync_worker_thread = None
        self.ui_queue = queue.Queue()

        self.live_sync_worker_thread = None
        self.live_sync_client_labels = {} # To hold labels for live updates
        self.discovered_oldest_date = None
        self.create_client_management_frame()
        self.create_eta_setup_frame()
        self.create_db_setup_frame()
        self.create_main_sync_frame()
        self.create_log_and_progress_frame()

        self.create_live_sync_frame()

        self.load_clients_from_config()
        self.after(100, self.process_queue)
        self.show_frame(self.eta_frame)

    # --- UI Creation (mostly the same, with key changes) ---
    def create_eta_setup_frame(self):
        self.eta_frame = ctk.CTkFrame(self)
        self.eta_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=0)
        self.eta_frame.grid_columnconfigure(0, weight=1)
        self.eta_frame.grid_columnconfigure(1, weight=1)
        
        ctk.CTkLabel(self.eta_frame, text="Step 1: ETA Credentials", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.client_id_entry = ctk.CTkEntry(self.eta_frame, placeholder_text="ETA Client ID")
        self.client_id_entry.grid(row=1, column=0, columnspan=2, padx=10, pady=5, sticky="ew")
        self.client_secret_entry = ctk.CTkEntry(self.eta_frame, placeholder_text="ETA Client Secret", show="*")
        self.client_secret_entry.grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="ew")

        # --- NEW: Two separate buttons for better UX ---
        self.eta_test_button = ctk.CTkButton(self.eta_frame, text="Test Authentication", command=self.run_eta_auth_test)
        self.eta_test_button.grid(row=3, column=0, padx=10, pady=15, sticky="e")
        self.eta_analyze_button = ctk.CTkButton(self.eta_frame, text="Analyze Invoice Dates", state="disabled", command=self.run_eta_analysis)
        self.eta_analyze_button.grid(row=3, column=1, padx=10, pady=15, sticky="w")
        
        self.eta_status_label = ctk.CTkLabel(self.eta_frame, text="Status: Awaiting credentials...", text_color="gray")
        self.eta_status_label.grid(row=4, column=0, columnspan=2, pady=5)
    # ... (other create_* methods from previous version can be reused here) ...
    def create_client_management_frame(self):
        self.client_frame = ctk.CTkFrame(self)
        self.client_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        self.client_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.client_frame, text="Select Client:", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, padx=10)
        self.client_selector = ctk.CTkOptionMenu(self.client_frame, variable=self.selected_client_name, command=self.on_client_selected)
        self.client_selector.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        self.client_name_entry = ctk.CTkEntry(self.client_frame, placeholder_text="Enter New or Existing Client Name")
        self.client_name_entry.grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="ew")
        self.export_button = ctk.CTkButton(self.client_frame, text="Export Clients", width=120, command=self.export_clients)
        self.export_button.grid(row=0, column=2, padx=10, pady=10)
        self.client_name_entry = ctk.CTkEntry(self.client_frame, placeholder_text="Enter New or Existing Client Name")
        self.client_name_entry.grid(row=1, column=0, columnspan=3, padx=10, pady=(0, 10), sticky="ew")
        self.go_to_live_sync_button = ctk.CTkButton(self.client_frame, text="Go to Live Sync", width=120, command=lambda: self.show_frame(self.live_sync_frame))
        self.go_to_live_sync_button.grid(row=0, column=3, padx=10, pady=10)


    # --- NEW: Export function ---
    def export_clients(self):
        clients = config_manager.load_all_clients()
        if not clients:
            messagebox.showinfo("Export Clients", "No clients are configured to export.")
            return

        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Save Client Export"
        )
        if not filepath:
            return

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                # Define all possible fields, including the new ones
                fieldnames = [
                    'client_name', 'client_id', 'client_secret', 'db_host', 'db_port', 
                    'db_name', 'db_user', 'db_pass', 'oldest_invoice_date', 'date_span'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for name, data in clients.items():
                    row = {'client_name': name}
                    row.update(data)
                    writer.writerow(row)
            messagebox.showinfo("Export Successful", f"Successfully exported {len(clients)} clients to:\n{filepath}")
        except IOError as e:
            messagebox.showerror("Export Failed", f"An error occurred while saving the file:\n{e}")
    def create_db_setup_frame(self):
        self.db_frame = ctk.CTkFrame(self)
        self.db_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(self.db_frame, text="Step 2: Database Connection", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=4, pady=10)
        self.db_host_entry = ctk.CTkEntry(self.db_frame, placeholder_text="DB Host")
        self.db_host_entry.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
        self.db_user_entry = ctk.CTkEntry(self.db_frame, placeholder_text="DB User")
        self.db_user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        self.db_pass_entry = ctk.CTkEntry(self.db_frame, placeholder_text="DB Password", show="*")
        self.db_pass_entry.grid(row=1, column=2, padx=5, pady=5, sticky="ew")
        self.db_name_entry = ctk.CTkEntry(self.db_frame, placeholder_text="DB Name")
        self.db_name_entry.grid(row=1, column=3, padx=5, pady=5, sticky="ew")
        self.db_test_button = ctk.CTkButton(self.db_frame, text="Test & Save Connection", command=self.run_db_test)
        self.db_test_button.grid(row=2, column=0, columnspan=4, pady=15)
        self.db_status_label = ctk.CTkLabel(self.db_frame, text="Status: Awaiting credentials...", text_color="gray")
        self.db_status_label.grid(row=3, column=0, columnspan=4, pady=5)
    def create_main_sync_frame(self):
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.grid_columnconfigure(4, weight=1)
        ctk.CTkLabel(self.main_frame, text="Step 3: Run Sync", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=6, pady=10)
        self.start_date_label = ctk.CTkLabel(self.main_frame, text="Start Date:")
        self.start_date_label.grid(row=1, column=0, padx=5, pady=5)
        self.start_date_entry = DateEntry(self.main_frame, date_pattern='y-mm-dd')
        self.start_date_entry.grid(row=1, column=1, padx=5, pady=5)
        self.end_date_label = ctk.CTkLabel(self.main_frame, text="End Date:")
        self.end_date_label.grid(row=1, column=2, padx=5, pady=5)
        self.end_date_entry = DateEntry(self.main_frame, date_pattern='y-mm-dd')
        self.end_date_entry.grid(row=1, column=3, padx=5, pady=5)
        self.sync_button = ctk.CTkButton(self.main_frame, text="Start Historical Sync", command=self.start_sync)
        self.sync_button.grid(row=1, column=4, padx=10, pady=10, sticky="e")
        self.cancel_button = ctk.CTkButton(self.main_frame, text="Cancel Sync", state="disabled", command=self.cancel_sync)
        self.cancel_button.grid(row=1, column=5, padx=10, pady=10)
    def create_log_and_progress_frame(self):
        self.log_frame = ctk.CTkFrame(self)
        self.log_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=(0, 10))
        self.log_frame.grid_rowconfigure(0, weight=1)
        self.log_frame.grid_columnconfigure(0, weight=1)
        self.log_textbox = ctk.CTkTextbox(self.log_frame, state="disabled", wrap="word")
        self.log_textbox.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.progressbar = ctk.CTkProgressBar(self, orientation="horizontal")
        self.progressbar.grid(row=4, column=0, sticky="ew", padx=10, pady=(0, 10))
        self.progressbar.set(0)

    # --- NEW: Worker Threads with improved logic ---
    def run_eta_auth_test(self):
        client_id, client_secret = self.client_id_entry.get(), self.client_secret_entry.get()
        if not client_id or not client_secret:
            self.eta_status_label.configure(text="Error: Client ID and Secret are required.", text_color="red")
            return
        self.eta_test_button.configure(state="disabled", text="Testing...")
        self.eta_analyze_button.configure(state="disabled")
        self.eta_status_label.configure(text="Status: Authenticating...", text_color="orange")
        self.api_client = ETAApiClient(client_id, client_secret)
        threading.Thread(target=self._eta_auth_worker).start()
        
    def _eta_auth_worker(self):
        # This worker now checks if analysis is even needed
        success, message = self.api_client.test_authentication()
        if not success:
            self.ui_queue.put(("ETA_AUTH_DONE", (False, message)))
            return

        # Check the config for the *currently entered* client ID
        client_name = self.client_name_entry.get()
        client_data = self.clients.get(client_name, {})
        
        # If the oldest date is already saved for this client, we can skip the slow analysis
        if client_data.get('oldest_invoice_date'):
            self.ui_queue.put(("SKIP_ANALYSIS", client_data))
        else:
            # Otherwise, we proceed as normal
            self.ui_queue.put(("ETA_AUTH_DONE", (True, "Authentication valid.")))

    def run_eta_analysis(self):
        self.eta_analyze_button.configure(state="disabled")
        threading.Thread(target=self._eta_analysis_worker).start()

    def _eta_analysis_worker(self):
        self.ui_queue.put(("ETA_STATUS_UPDATE", "Analyzing... Searching for newest invoice..."))
        newest = self.api_client.find_newest_invoice_date()
        self.ui_queue.put(("ETA_STATUS_UPDATE", "Analyzing... Searching for oldest invoice (can take a minute)..."))
        oldest = self.api_client.find_oldest_invoice_date()
        if oldest:
            self.discovered_oldest_date = oldest.strftime('%Y-%m-%d') # Store the date
        self.ui_queue.put(("ETA_ANALYZE_DONE", (oldest, newest)))

    def run_db_test(self):
        # ... (same as before)
        db_params = { "host": self.db_host_entry.get(), "user": self.db_user_entry.get(), "password": self.db_pass_entry.get(), "dbname": self.db_name_entry.get() }
        if not all(db_params.values()):
            self.db_status_label.configure(text="Error: All database fields are required.", text_color="red"); return
        self.db_test_button.configure(state="disabled", text="Testing..."); self.db_status_label.configure(text="Status: Connecting...", text_color="orange")
        self.db_manager = DatabaseManager(db_params)
        threading.Thread(target=self._db_test_worker).start()
    
    def _db_test_worker(self):
        if not self.db_manager.connect():
            self.ui_queue.put(("DB_CONNECT_FAIL", "Connection failed. Check credentials/network."))
            return
        
        self.ui_queue.put(("DB_STATUS_UPDATE", "Connected. Verifying/Creating tables..."))
        success = self.db_manager.check_and_create_tables()
        self.ui_queue.put(("DB_SCHEMA_DONE", success))

    # --- NEW: UI Update Logic with more states ---
    def process_queue(self):
        try:
            message_type, data = self.ui_queue.get_nowait()

            if message_type == "ETA_AUTH_DONE":
                success, message = data
                self.eta_test_button.configure(state="normal", text="Test Authentication")
                if success:
                    self.eta_status_label.configure(text="Success! Authentication valid. Ready to analyze dates.", text_color="green")
                    self.eta_analyze_button.configure(state="normal") # Enable next step
                else:
                    self.eta_status_label.configure(text=f"Error: {message}", text_color="red")

            elif message_type == "SKIP_ANALYSIS":
                client_data = data
                self.eta_test_button.configure(state="normal")
                self.eta_status_label.configure(text="Success! Using saved date range.", text_color="green")
                
                # Use the saved dates to populate the calendars
                oldest_str = client_data.get('oldest_invoice_date')
                # --- CRASH FIX: The data is already a list, no need for json.loads() ---
                date_span_list = client_data.get('date_span')
                if date_span_list and len(date_span_list) > 1:
                    newest_str = date_span_list[1]
                    self.start_date_entry.set_date(datetime.datetime.strptime(oldest_str, '%Y-%m-%d').date())
                    self.end_date_entry.set_date(datetime.datetime.strptime(newest_str, '%Y-%m-%d').date())
                
                self.show_frame(self.db_frame)

            elif message_type == "ETA_STATUS_UPDATE":
                self.eta_status_label.configure(text=f"Status: {data}", text_color="orange")
            
            elif message_type == "ETA_ANALYZE_DONE":
                oldest, newest = data
                self.eta_analyze_button.configure(state="normal")
                if oldest and newest:
                    date_span = (oldest.strftime('%Y-%m-%d'), newest.strftime('%Y-%m-%d'))
                    self.eta_status_label.configure(text=f"Analysis Complete! Found invoices from {date_span[0]} to {date_span[1]}", text_color="green")
                    self.start_date_entry.set_date(oldest.date())
                    self.end_date_entry.set_date(newest.date())
                    self.show_frame(self.db_frame)
                else:
                    self.eta_status_label.configure(text="Error: Could not find any invoices for this client.", text_color="red")
            
            elif message_type == "DB_CONNECT_FAIL":
                self.db_status_label.configure(text=f"Error: {data}", text_color="red")
                self.db_test_button.configure(state="normal", text="Test & Save Connection")
            
            elif message_type == "DB_STATUS_UPDATE":
                self.db_status_label.configure(text=f"Status: {data}", text_color="orange")

            elif message_type == "DB_SCHEMA_DONE":
                success = data
                self.db_test_button.configure(state="normal", text="Test & Save Connection")
                if success:
                    self.db_status_label.configure(text="Success! Database is ready. Saving configuration...", text_color="green")
                    # Save all credentials
                    client_name = self.client_name_entry.get()
                    if not client_name: client_name = f"Client-{self.client_id_entry.get()[:6]}"
                    date_span = (self.start_date_entry.get_date().strftime('%Y-%m-%d'), self.end_date_entry.get_date().strftime('%Y-%m-%d'))
                    config_manager.save_client_config(client_name, self.client_id_entry.get(), self.client_secret_entry.get(),
                                                      self.db_host_entry.get(), 5432, self.db_name_entry.get(), self.db_user_entry.get(),
                                                      self.db_pass_entry.get(), date_span, self.discovered_oldest_date)
                    self.load_clients_from_config()
                    self.selected_client_name.set(client_name)
                    self.show_frame(self.main_frame)
                else:
                    self.db_status_label.configure(text="Error: Failed to create database tables. Check permissions.", text_color="red")
            
            # ... (LOG and PROGRESS cases remain the same) ...
            elif message_type == "PROGRESS": self.progressbar.set(float(data))
            elif message_type == "LOG":
                self.log_message(data)
                if data == "Sync Finished!" or data == "Sync cancelled by user.":
                    self.sync_button.configure(state="normal"); self.cancel_button.configure(state="disabled")
            elif message_type == "LIVE_UPDATE":
                client_name, status_text = data
                if client_name in self.live_sync_client_labels:
                    self.live_sync_client_labels[client_name].configure(text=status_text)
                if "Done" in status_text or "Fail" in status_text:
                    if all("Done" in lbl.cget("text") or "Fail" in lbl.cget("text") for lbl in self.live_sync_client_labels.values()):
                        self.live_sync_start_button.configure(state="normal")
                        self.live_sync_cancel_button.configure(state="disabled")
        except queue.Empty: pass
        finally: self.after(100, self.process_queue)

    def create_live_sync_frame(self):
        self.live_sync_frame = ctk.CTkFrame(self)
        self.live_sync_frame.grid_columnconfigure(0, weight=1)

        header_frame = ctk.CTkFrame(self.live_sync_frame)
        header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        header_frame.grid_columnconfigure(0, weight=1)

        self.live_sync_start_button = ctk.CTkButton(header_frame, text="Start Live Sync", command=self.start_live_sync)
        self.live_sync_start_button.grid(row=0, column=0, padx=10, pady=10)
        self.live_sync_cancel_button = ctk.CTkButton(header_frame, text="Cancel", state="disabled", command=self.cancel_live_sync)
        self.live_sync_cancel_button.grid(row=0, column=1, padx=10, pady=10)
        self.back_to_setup_button = ctk.CTkButton(header_frame, text="< Back to Setup", command=lambda: self.show_frame(self.eta_frame))
        self.back_to_setup_button.grid(row=0, column=2, padx=10, pady=10)

        self.client_list_frame = ctk.CTkScrollableFrame(self.live_sync_frame, label_text="Client Sync Status")
        self.client_list_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.live_sync_frame.grid_rowconfigure(1, weight=1)

    def populate_live_sync_frame(self):
        # Clear previous labels
        for widget in self.client_list_frame.winfo_children():
            widget.destroy()
        self.live_sync_client_labels = {}

        # --- CRASH FIX: Use the correct parameter mapping for the DB connection ---
        db_params = {
            'host': self.db_host_entry.get(),
            'dbname': self.db_name_entry.get(),
            'user': self.db_user_entry.get(),
            'password': self.db_pass_entry.get(),
            'port': 5432 # Assuming default, could be loaded from config too
        }
        temp_db_manager = DatabaseManager(db_params)
        statuses = {}
        if temp_db_manager.connect():
            # Now we need a richer get_all_sync_statuses method
            statuses = temp_db_manager.get_all_sync_statuses() 
            temp_db_manager.disconnect()

        for i, (name, config) in enumerate(self.clients.items()):
            client_id = config.get('client_id')
            last_sync_info = statuses.get(client_id)
            
            # --- UPGRADED UI DISPLAY ---
            if last_sync_info:
                ts, uuid, internal_id = last_sync_info
                sync_text = f"Up to doc {internal_id or uuid[:8]} on {ts.strftime('%Y-%m-%d')}"
            else:
                sync_text = "Never Synced"

            ctk.CTkLabel(self.client_list_frame, text=name, font=ctk.CTkFont(weight="bold")).grid(row=i, column=0, sticky="w", padx=10)
            status_label = ctk.CTkLabel(self.client_list_frame, text=sync_text, anchor="e")
            status_label.grid(row=i, column=1, sticky="e", padx=10)
            self.live_sync_client_labels[name] = status_label
    # --- Other methods (show_frame, load_clients, on_client_selected, etc.) can be reused ---
    def show_frame(self, frame_to_show):
        self.eta_frame.grid_remove(); self.db_frame.grid_remove(); self.main_frame.grid_remove()
        if frame_to_show == self.main_frame:
             frame_to_show.grid(row=2, column=0, sticky="ew", padx=10, pady=0)
        else:
             frame_to_show.grid(row=1, column=0, sticky="ew", padx=10, pady=0)

        if frame_to_show == self.live_sync_frame:
            self.populate_live_sync_frame()
            self.live_sync_frame.grid(row=1, column=0, rowspan=2, sticky="nsew", padx=10, pady=0)

    def load_clients_from_config(self):
        self.clients = config_manager.load_all_clients()
        client_names = list(self.clients.keys()) if self.clients else ["No clients configured"]
        self.client_selector.configure(values=client_names)
        last_client = config_manager.load_last_selected_client()
        if last_client and last_client in self.clients:
            self.selected_client_name.set(last_client); self.on_client_selected(last_client)
        elif client_names[0] != "No clients configured":
             self.selected_client_name.set(client_names[0]); self.on_client_selected(client_names[0])
        else:
             self.selected_client_name.set(client_names[0])
    def on_client_selected(self, selected_name):
        if selected_name in self.clients:
            client_data = self.clients[selected_name]
            self.client_name_entry.delete(0, "end"); self.client_name_entry.insert(0, selected_name)
            self.client_id_entry.delete(0, "end"); self.client_id_entry.insert(0, client_data.get('client_id', ''))
            self.client_secret_entry.delete(0, "end"); self.client_secret_entry.insert(0, client_data.get('client_secret', ''))
            self.db_host_entry.delete(0, "end"); self.db_host_entry.insert(0, client_data.get('db_host', ''))
            self.db_user_entry.delete(0, "end"); self.db_user_entry.insert(0, client_data.get('db_user', ''))
            self.db_pass_entry.delete(0, "end"); self.db_pass_entry.insert(0, client_data.get('db_pass', ''))
            self.db_name_entry.delete(0, "end"); self.db_name_entry.insert(0, client_data.get('db_name', ''))
            # --- THIS IS THE NEW LOGIC ---
            # If we already know the oldest date, we don't need to analyze again.
            if client_data.get('oldest_invoice_date'):
                self.eta_analyze_button.configure(text="Dates Already Analyzed", state="disabled")
            else:
                self.eta_analyze_button.configure(text="Analyze Invoice Dates", state="disabled") # Disabled until auth is tested

            config_manager.save_last_selected_client(selected_name)
            self.show_frame(self.eta_frame)

            config_manager.save_last_selected_client(selected_name)
            self.show_frame(self.eta_frame)
    def start_sync(self):
        self.progressbar.set(0); self.log_message("--- Starting Sync ---")
        self.sync_button.configure(state="disabled"); self.cancel_button.configure(state="normal")
        start_date, end_date = self.start_date_entry.get_date(), self.end_date_entry.get_date()
        self.sync_worker_thread = SyncWorker(self.api_client.client_id, self.api_client, self.db_manager, start_date, end_date, self.ui_queue)
        self.sync_worker_thread.start()
    def cancel_sync(self):
        if self.sync_worker_thread and self.sync_worker_thread.is_alive():
            self.sync_worker_thread.stop(); self.log_message("--- Cancellation requested ---"); self.cancel_button.configure(state="disabled")
    def log_message(self, message):
        self.log_textbox.configure(state="normal")
        self.log_textbox.insert("end", f"{datetime.datetime.now().strftime('%H:%M:%S')} - {message}\n")
        self.log_textbox.configure(state="disabled")
        self.log_textbox.see("end")
    
    def start_live_sync(self):
        self.live_sync_start_button.configure(state="disabled")
        self.live_sync_cancel_button.configure(state="normal")
        self.live_sync_worker_thread = LiveSyncWorker(self.clients, self.ui_queue)
        self.live_sync_worker_thread.start()
    def cancel_live_sync(self):
        if self.live_sync_worker_thread and self.live_sync_worker_thread.is_alive():
            self.live_sync_worker_thread.stop()
            self.log_message("--- Live Sync cancellation requested ---")
            self.live_sync_cancel_button.configure(state="disabled")


if __name__ == "__main__":
    app = App()
    app.mainloop()