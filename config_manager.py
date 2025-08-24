import configparser
import os
import json

CONFIG_FILE = 'settings.ini'

def save_client_config(client_name, client_id, client_secret, db_host, db_port, db_name, db_user, db_pass, date_span=None, oldest_invoice_date=None):
    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)
    section_name = f"Client_{client_name}"
    config[section_name] = {
        'client_id': client_id,
        'client_secret': client_secret,
        'db_host': db_host,
        'db_port': str(db_port),
        'db_name': db_name,
        'db_user': db_user,
        'db_pass': db_pass,
        'date_span': json.dumps(date_span) if date_span else "",
        'oldest_invoice_date': oldest_invoice_date if oldest_invoice_date else ""
    }
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)

def load_all_clients():
    if not os.path.exists(CONFIG_FILE): return {}
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    clients = {}
    for section in config.sections():
        if section.startswith('Client_'):
            client_name = section.replace('Client_', '', 1)
            clients[client_name] = dict(config.items(section))
            try:
                clients[client_name]['date_span'] = json.loads(clients[client_name].get('date_span', 'null'))
            except (json.JSONDecodeError, TypeError):
                clients[client_name]['date_span'] = None
    return clients

def save_last_selected_client(client_name):
    """Saves the name of the last used client."""
    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)
    
    if 'AppState' not in config:
        config['AppState'] = {}
    config['AppState']['last_client'] = client_name

    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)

def load_last_selected_client():
    """Loads the name of the last used client."""
    if not os.path.exists(CONFIG_FILE):
        return None
    
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config.get('AppState', 'last_client', fallback=None)