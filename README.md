ETA-Fetcher User Guide
Welcome to ETA-Fetcher! This guide will walk you through setting up and using the software to automatically download your company's e-invoices from the Egyptian Tax Authority (ETA).
1. What is ETA-Fetcher?
ETA-Fetcher is a desktop application designed to securely connect to the ETA's online system and download your company's sales (sent) and purchase (received) invoices. It saves them directly into a database, saving you hours of manual work and providing a permanent, searchable record of your transactions.
2. Before You Begin: What You Need
To use ETA-Fetcher, you will need to have the following three pieces of information ready. You can get these from your company's IT department or from your account on the ETA e-invoicing portal.
Your Company Tax ID: Your company's 9-digit tax registration number (e.g., 123456789).
Your API Client ID: A long, unique code used to identify this software.
Your API Client Secret: A secure password that works with the Client ID.
You will also need the connection details for the database where the invoices will be stored.
3. Installation
To install ETA-Fetcher, simply run the ETA-Fetcher-Setup.exe file.
You will be asked for an installation password. Please enter the password provided to you to begin the installation.
Follow the on-screen instructions. We recommend allowing the installer to create a desktop shortcut for easy access.
4. First-Time Setup: A Step-by-Step Guide
The first time you run ETA-Fetcher, you will be guided through a simple setup process to configure your company's details.
Step 1: Select or Create a Client
A "Client" is simply a profile for a company you want to fetch invoices for.
In the "Enter New or Existing Client Name" box, type a name you will recognize, for example, "My Trading Company".
Step 2: Enter and Test Your ETA Credentials
Fill in the following fields with the information you gathered in section 2:
Company Tax ID: Your 9-digit tax number.
API Client ID: Your unique API identifier.
API Client Secret: Your secret password.
Once the fields are filled, click the "Test Authentication" button.
Success: The status message will turn green and say "Success! Authentication valid." This means the software can connect to the ETA. The "Analyze Invoice Dates" button will become clickable.
Failure: The status will turn red. Please double-check that you have copied and pasted the credentials correctly, with no extra spaces.
Step 3: Analyze Your Invoice History
Click the "Analyze Invoice Dates" button. The software will securely communicate with the ETA to find the date of your very first and most recent invoices. This may take up to a minute.
Success: The status will turn green and show you the date range of your invoices. You will automatically be taken to the next step.
Failure: If no invoices are found, please verify that your company has issued or received e-invoices.
Step 4: Connect to Your Database
Now, you need to tell the software where to save the invoices. Fill in the database connection details provided by your IT department:
DB Host: The server name or IP address.
DB User: The database username.
DB Password: The database password.
DB Name: The name of the database.
Click the "Test & Save Connection" button.
Success: The status will turn green. If the database is new, the software will automatically create the necessary tables for you. Your client profile is now saved, and you will be taken to the final Sync screen.
Failure: The status will turn red. Please double-check the database details.
If you ever need to create a new, empty database, you can enter its desired name in the "DB Name" field and click "Create Database".
5. Using ETA-Fetcher
Once set up, using the application is simple.
Fetching All Past Invoices (Historical Sync)
This is usually done only once for a new client.
The Start Date and End Date will be automatically filled from the analysis step.
Click the "Start Historical Sync" button.
You can monitor the progress in the log window at the bottom of the application. This process can take a long time depending on how many invoices you have. You can cancel it at any time.
Fetching New Invoices (Live Sync)
This is the mode you will use for daily or regular updates.
From the main setup screen, click the "Go to Live Sync" button.
You will see a list of all your configured clients and the date of their last sync.
Click "Start Live Sync". The software will automatically check each client and download only the invoices that have arrived since the last sync.
When finished, you can click "Refresh Status" to see the updated last sync dates.
