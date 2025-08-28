# main.py
import os
from tkinter import messagebox
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
        self.current_logfile = None
        os.makedirs("logs", exist_ok=True) # Creates the 'logs' directory if it doesn't exist
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
        
        # --- NEW: Added a dedicated field for the Tax ID ---
        self.client_id_entry = ctk.CTkEntry(self.eta_frame, placeholder_text="API Client ID")
        self.client_id_entry.grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="ew")
        self.client_secret_entry = ctk.CTkEntry(self.eta_frame, placeholder_text="API Client Secret", show="*")
        self.client_secret_entry.grid(row=3, column=0, columnspan=2, padx=10, pady=5, sticky="ew")

        self.eta_test_button = ctk.CTkButton(self.eta_frame, text="Test Authentication", command=self.run_eta_auth_test)
        self.eta_test_button.grid(row=4, column=0, padx=10, pady=15, sticky="e")
        self.eta_analyze_button = ctk.CTkButton(self.eta_frame, text="Analyze Invoice Dates", state="disabled", command=self.run_eta_analysis)
        self.eta_analyze_button.grid(row=4, column=1, padx=10, pady=15, sticky="w")
        
        self.eta_status_label = ctk.CTkLabel(self.eta_frame, text="Status: Awaiting credentials...", text_color="gray")
        self.eta_status_label.grid(row=5, column=0, columnspan=2, pady=5)
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
                    'db_name', 'db_user', 'db_pass', 'oldest_invoice_date', 'date_span','skipped_days'
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
        self.db_frame.grid_columnconfigure(0, weight=1) # Make the button frame centered

        ctk.CTkLabel(self.db_frame, text="Step 2: Database Connection", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=4, pady=10)
        
        # Grid for entry fields
        entry_frame = ctk.CTkFrame(self.db_frame, fg_color="transparent")
        entry_frame.grid(row=1, column=0, sticky="ew")
        entry_frame.grid_columnconfigure((0,1,2,3), weight=1)
        
        self.db_host_entry = ctk.CTkEntry(entry_frame, placeholder_text="DB Host")
        self.db_host_entry.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        self.db_user_entry = ctk.CTkEntry(entry_frame, placeholder_text="DB User")
        self.db_user_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.db_pass_entry = ctk.CTkEntry(entry_frame, placeholder_text="DB Password", show="*")
        self.db_pass_entry.grid(row=0, column=2, padx=5, pady=5, sticky="ew")
        self.db_name_entry = ctk.CTkEntry(entry_frame, placeholder_text="DB Name")
        self.db_name_entry.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

        # Grid for buttons
        button_frame = ctk.CTkFrame(self.db_frame, fg_color="transparent")
        button_frame.grid(row=2, column=0, pady=15)

        # --- NEW: Back Button ---
        self.db_back_button = ctk.CTkButton(button_frame, text="< Back to ETA", command=lambda: self.show_frame(self.eta_frame))
        self.db_back_button.grid(row=0, column=0, padx=10)
        
        self.db_test_button = ctk.CTkButton(button_frame, text="Test & Save Connection", command=self.run_db_test)
        self.db_test_button.grid(row=0, column=1, padx=10)
        self.db_create_button = ctk.CTkButton(button_frame, text="Create Database", command=self.run_db_create)
        self.db_create_button.grid(row=0, column=2, padx=10)
        
        self.db_status_label = ctk.CTkLabel(self.db_frame, text="Status: Awaiting credentials...", text_color="gray")
        self.db_status_label.grid(row=3, column=0, columnspan=4, pady=5)

    def create_main_sync_frame(self):
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(self.main_frame, text="Step 3: Run Sync", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, pady=10)

        # Frame for date pickers
        date_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        date_frame.grid(row=1, column=0)
        
        self.start_date_label = ctk.CTkLabel(date_frame, text="Start Date:")
        self.start_date_label.grid(row=0, column=0, padx=5, pady=5)
        self.start_date_entry = DateEntry(date_frame, date_pattern='y-mm-dd')
        self.start_date_entry.grid(row=0, column=1, padx=5, pady=5)
        self.end_date_label = ctk.CTkLabel(date_frame, text="End Date:")
        self.end_date_label.grid(row=0, column=2, padx=(20, 5), pady=5)
        self.end_date_entry = DateEntry(date_frame, date_pattern='y-mm-dd')
        self.end_date_entry.grid(row=0, column=3, padx=5, pady=5)

        # Frame for action buttons
        button_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        button_frame.grid(row=2, column=0, pady=15)
        
        # --- NEW: Back Button ---
        self.sync_back_button = ctk.CTkButton(button_frame, text="< Back to Database", command=lambda: self.show_frame(self.db_frame))
        self.sync_back_button.grid(row=0, column=0, padx=10)

        self.sync_button = ctk.CTkButton(button_frame, text="Start Historical Sync", command=self.start_sync)
        self.sync_button.grid(row=0, column=1, padx=10)
        self.cancel_button = ctk.CTkButton(button_frame, text="Cancel Sync", state="disabled", command=self.cancel_sync)
        self.cancel_button.grid(row=0, column=2, padx=10)
        
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
        # --- NEW: Create and set the log file for this session ---
        client_name_safe = self.selected_client_name.get().replace(" ", "_").replace("/", "-")
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.current_logfile = f"logs/Historical_{client_name_safe}_{timestamp}.txt"

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

            elif message_type == "DB_CREATE_DONE":
                success, message = data
                self.db_create_button.configure(state="normal", text="Create Database")
                self.db_test_button.configure(state="normal")
                
                if success:
                    self.db_status_label.configure(text=f"Success: {message} Ready to test connection.", text_color="green")
                    # For good UX, let's automatically test the connection now
                    self.db_test_button.invoke() 
                else:
                    self.db_status_label.configure(text=f"Info: {message}", text_color="orange") # Use orange for "already exists"

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
                    # This part is correct, it updates the 'status' label
                    self.live_sync_client_labels[client_name]['status'].configure(text=status_text)
                
                # --- THIS IS THE CRASH FIX ---
                # We need to check the 'status' label inside each dictionary value
                if all("Done" in lbl_dict['status'].cget("text") or "Fail" in lbl_dict['status'].cget("text") for lbl_dict in self.live_sync_client_labels.values()):
                    self.live_sync_start_button.configure(state="normal")
                    self.live_sync_cancel_button.configure(state="disabled")
                    self.live_sync_refresh_button.configure(state="normal") # Also re-enable refresh
                    
            elif message_type == "HISTORICAL_SYNC_COMPLETE":
                skipped_days = data
                final_message = f"Sync Finished! Found {len(skipped_days)} skipped days to retry later."
                self.log_message(final_message)
                
                # Save the new list of skipped days to the config
                client_name = self.client_name_entry.get()
                if client_name in self.clients:
                    client_data = self.clients[client_name]
                    # Merge old and new skipped days, removing duplicates
                    existing_skipped = set(client_data.get('skipped_days', []))
                    new_skipped = set(skipped_days)
                    final_skipped_list = sorted(list(existing_skipped | new_skipped))
                    
                    # Call the save function with all the required data
                    config_manager.save_client_config(
                        client_name, client_data.get('client_id'), client_data.get('client_secret'),
                        client_data.get('db_host'), client_data.get('db_port'), client_data.get('db_name'),
                        client_data.get('db_user'), client_data.get('db_pass'), client_data.get('date_span'),
                        client_data.get('oldest_invoice_date'), final_skipped_list
                    )
                    self.load_clients_from_config() # Refresh in-memory client data
                
                self.sync_button.configure(state="normal")
                self.cancel_button.configure(state="disabled")
                messagebox.showinfo("Historical Sync", final_message)
                self.current_logfile = None # --- NEW: Close the log file ---

            elif message_type == "LIVE_SYNC_COMPLETE":
                self.live_sync_start_button.configure(state="normal")
                self.live_sync_cancel_button.configure(state="disabled")
                self.live_sync_refresh_button.configure(state="normal")
                messagebox.showinfo("Live Sync", "Live sync for all clients is complete.")
                self.current_logfile = None # --- NEW: Close the log file ---
        except queue.Empty: pass
        finally: self.after(100, self.process_queue)

    def create_live_sync_frame(self):
        self.live_sync_frame = ctk.CTkFrame(self)
        self.live_sync_frame.grid_columnconfigure(0, weight=1)

        header_frame = ctk.CTkFrame(self.live_sync_frame)
        header_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        header_frame.grid_columnconfigure(1, weight=1)

        self.live_sync_start_button = ctk.CTkButton(header_frame, text="Start Live Sync", command=self.start_live_sync)
        self.live_sync_start_button.grid(row=0, column=0, padx=10, pady=10)
        
        self.live_sync_refresh_button = ctk.CTkButton(header_frame, text="Refresh Status", command=self.populate_live_sync_frame)
        self.live_sync_refresh_button.grid(row=0, column=1, padx=(20,10), pady=10, sticky="w")
        
        self.live_sync_cancel_button = ctk.CTkButton(header_frame, text="Cancel", state="disabled", command=self.cancel_live_sync)
        self.live_sync_cancel_button.grid(row=0, column=2, padx=10, pady=10)
        
        self.back_to_setup_button = ctk.CTkButton(header_frame, text="< Back to Setup", command=lambda: self.show_frame(self.eta_frame))
        self.back_to_setup_button.grid(row=0, column=3, padx=10, pady=10)

        self.client_list_frame = ctk.CTkScrollableFrame(self.live_sync_frame, label_text="Client Sync Status")
        self.client_list_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.live_sync_frame.grid_rowconfigure(1, weight=1)
        self.client_list_frame.grid_columnconfigure(2, weight=1)

    def populate_live_sync_frame(self):
        """
        Prepares the Live Sync UI by connecting to EACH client's database
        to fetch their individual, accurate sync status.
        """
        for widget in self.client_list_frame.winfo_children():
            widget.destroy()
        self.live_sync_client_labels = {}

        if not self.clients:
            ctk.CTkLabel(self.client_list_frame, text="No clients have been configured yet.").pack(pady=20)
            self.live_sync_start_button.configure(state="disabled")
            return

        self.live_sync_start_button.configure(state="normal")
        
        # --- NEW LOGIC: Iterate through each client and connect to their DB ---
        for i, (name, config) in enumerate(self.clients.items()):
            db_params = {
                'host': config.get('db_host'), 'dbname': config.get('db_name'),
                'user': config.get('db_user'), 'password': config.get('db_pass'),
                'port': config.get('db_port', 5432)
            }
            # This key was named 'tax_id' previously, but should be 'client_id' from the API
            client_api_id = config.get('client_id') 
            
            # Create a temporary DB manager for this client
            temp_db_manager = DatabaseManager(db_params)
            sync_text = "DB Conn Fail" # Default text
            
            if temp_db_manager.connect():
                statuses = temp_db_manager.get_all_sync_statuses()
                temp_db_manager.disconnect()
                # Use the client's API ID to look up their status
                last_sync_info = statuses.get(client_api_id)
                if last_sync_info:
                    ts, uuid, internal_id = last_sync_info
                # --- THIS IS THE CRASH FIX ---
                # Build the display text safely, handling None for uuid/internal_id
                doc_identifier = internal_id or (uuid[:8] if uuid else None)
                if doc_identifier:
                    sync_text = f"Up to doc '{doc_identifier}' on {ts.strftime('%Y-%m-%d')}"
                else:
                    # Fallback if both are None, which can happen after a historical sync
                    sync_text = f"Synced up to {ts.strftime('%Y-%m-%d %H:%M')}"
            else:
                sync_text = "Never Synced"

            # Now create the UI labels with the fetched info
            ctk.CTkLabel(self.client_list_frame, text=name, font=ctk.CTkFont(weight="bold")).grid(row=i, column=0, sticky="w", padx=10, pady=5)
            # --- TYPO FIX: Changed ck to ctk ---
            last_sync_label = ctk.CTkLabel(self.client_list_frame, text=sync_text, anchor="w")
            last_sync_label.grid(row=i, column=1, sticky="w", padx=10)
            status_label = ctk.CTkLabel(self.client_list_frame, text="Idle", text_color="gray", anchor="e")
            status_label.grid(row=i, column=2, sticky="ew", padx=10)
            
            self.live_sync_client_labels[name] = {'main': last_sync_label, 'status': status_label}

    def show_frame(self, frame_to_show):
        """Hides all main content frames and shows the specified one in the correct layout."""
        # --- THE CRITICAL FIX: Explicitly hide ALL possible main content frames ---
        self.eta_frame.grid_remove()
        self.db_frame.grid_remove()
        self.main_frame.grid_remove()
        self.live_sync_frame.grid_remove()

        # Now, show the requested frame in its correct grid position
        if frame_to_show == self.live_sync_frame:
            # The Live Sync page takes up the full area below the client selector
            self.populate_live_sync_frame()
            self.live_sync_frame.grid(row=1, column=0, rowspan=2, sticky="nsew", padx=10, pady=0)
        
        elif frame_to_show == self.main_frame:
            # The Main Sync page is the third step in the setup workflow
            self.main_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=0)
        
        else: # This handles both the eta_frame and the db_frame
            # These are the first and second steps in the setup workflow
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
        """Called when a client is chosen. Populates fields and resets the view."""
        if selected_name in self.clients:
            client_data = self.clients[selected_name]
            self.client_name_entry.delete(0, "end")
            self.client_name_entry.insert(0, selected_name)
            # Populate all fields from saved config
            self.client_id_entry.delete(0, "end"); self.client_id_entry.insert(0, client_data.get('client_id', ''))
            self.client_secret_entry.delete(0, "end"); self.client_secret_entry.insert(0, client_data.get('client_secret', ''))
            self.db_host_entry.delete(0, "end"); self.db_host_entry.insert(0, client_data.get('db_host', ''))
            self.db_user_entry.delete(0, "end"); self.db_user_entry.insert(0, client_data.get('db_user', ''))
            self.db_pass_entry.delete(0, "end"); self.db_pass_entry.insert(0, client_data.get('db_pass', ''))
            self.db_name_entry.delete(0, "end"); self.db_name_entry.insert(0, client_data.get('db_name', ''))
            
            # --- NEW: Reset status labels and the view ---
            self.eta_status_label.configure(text="Status: Awaiting credentials...", text_color="gray")
            self.db_status_label.configure(text="Status: Awaiting credentials...", text_color="gray")
            self.eta_analyze_button.configure(state="disabled") # Re-disable analyze button until auth is tested
            
            config_manager.save_last_selected_client(selected_name)
            # --- NEW: Always return to the first step for the newly selected client ---
            self.show_frame(self.eta_frame)

    def start_sync(self):
        self.progressbar.set(0); self.log_message("--- Starting Sync ---")
        self.sync_button.configure(state="disabled"); self.cancel_button.configure(state="normal")
        start_date, end_date = self.start_date_entry.get_date(), self.end_date_entry.get_date()
        self.sync_worker_thread = SyncWorker(self.api_client.client_id, self.api_client, self.db_manager, start_date, end_date, self.ui_queue)
        self.sync_worker_thread.start()
        
        # --- NEW: Create and set the log file for this session (Robust version) ---
        # 1. Try the text entry box first, as it might have been edited.
        client_name = self.client_name_entry.get()
        # 2. If it's empty, fall back to the dropdown's selected value.
        if not client_name:
            client_name = self.selected_client_name.get()
        # 3. As a final failsafe, if it's still invalid, use a default.
        if not client_name or client_name == "No clients configured":
            client_name = "Untitled_Client"
        
        client_name_safe = client_name.replace(" ", "_").replace("/", "-")
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.current_logfile = f"logs/Historical_{client_name_safe}_{timestamp}.txt"
    def cancel_sync(self):
        if self.sync_worker_thread and self.sync_worker_thread.is_alive():
            self.sync_worker_thread.stop(); self.log_message("--- Cancellation requested ---"); self.cancel_button.configure(state="disabled")
            self.current_logfile = None # --- NEW: Close the log file on cancellation ---
    def log_message(self, message):
        """Appends a message to the UI textbox AND the external log file."""
        should_autoscroll = self.log_textbox.yview()[1] == 1.0
        
        # --- NEW: Write to external log file first ---
        if self.current_logfile:
            try:
                with open(self.current_logfile, 'a', encoding='utf-8') as f:
                    # Write the full, timestamped message to the file
                    f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
            except Exception as e:
                # If logging fails, print to console but don't crash the app
                print(f"!!! FAILED TO WRITE TO LOG FILE {self.current_logfile}: {e}")

        # Update the on-screen textbox
        self.log_textbox.configure(state="normal")
        self.log_textbox.insert("end", f"{datetime.datetime.now().strftime('%H:%M:%S')} - {message}\n")
        self.log_textbox.configure(state="disabled")

        if should_autoscroll:
            self.log_textbox.see("end")
    
    def start_live_sync(self):
        self.live_sync_start_button.configure(state="disabled")
        # --- NEW: Create and set the log file for this session ---
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        self.current_logfile = f"logs/LiveSync_AllClients_{timestamp}.txt"
        self.live_sync_cancel_button.configure(state="normal")
        self.live_sync_refresh_button.configure(state="disabled") # Disable refresh during sync
        self.live_sync_worker_thread = LiveSyncWorker(self.clients, self.ui_queue)
        self.live_sync_worker_thread.daemon = True # This is the critical fix for not stopping.
        self.live_sync_worker_thread.start()
    def cancel_live_sync(self):
        if self.live_sync_worker_thread and self.live_sync_worker_thread.is_alive():
            self.live_sync_worker_thread.stop()
            self.log_message("--- Live Sync cancellation requested ---")
            self.live_sync_cancel_button.configure(state="disabled")
            self.current_logfile = None # --- NEW: Close the log file on cancellation ---

    def run_db_create(self):
        """Starts the database creation worker thread."""
        db_name = self.db_name_entry.get()
        if not db_name:
            self.db_status_label.configure(text="Error: DB Name field cannot be empty to create a database.", text_color="red")
            return
            
        db_params = { "host": self.db_host_entry.get(), "user": self.db_user_entry.get(), "password": self.db_pass_entry.get(), "port": 5432 }
        if not all(db_params.values()):
            self.db_status_label.configure(text="Error: Host, User, and Password fields are required.", text_color="red")
            return

        self.db_test_button.configure(state="disabled")
        self.db_create_button.configure(state="disabled", text="Creating...")
        self.db_status_label.configure(text=f"Status: Attempting to create '{db_name}'...", text_color="orange")

        # We don't need a full DBManager instance, just the connection params
        threading.Thread(target=self._db_create_worker, args=(db_params, db_name)).start()

    def _db_create_worker(self, db_params, db_name):
        """Worker that calls the new creation logic."""
        # We create a temporary manager instance just for this operation
        temp_db_manager = DatabaseManager(db_params)
        success, message = temp_db_manager.create_database(db_name)
        self.ui_queue.put(("DB_CREATE_DONE", (success, message)))

if __name__ == "__main__":
    app = App()
    app.mainloop()