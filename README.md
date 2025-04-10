# WDA Monitor

A web-based monitoring system for tracking and managing parts data across various modules and manufacturers.

## Overview

WDA Monitor is a Flask-based web application designed to track the status of parts across different modules and manufacturers. It provides a comprehensive dashboard for monitoring, visualizing, and managing parts data with features for uploading, checking status, and generating reports.

## Features

- **Parts Management**: Upload, delete, and monitor parts data
- **Status Tracking**: Check the status of parts across different modules
- **Visualization Dashboard**: Interactive charts and graphs for data analysis
- **Import Status Monitoring**: Track import status of parts
- **Automated Tasks**: Scheduled daily checks and matrix downloads
- **Amazon Integration**: Upload filtered data to Amazon
- **Export Capabilities**: Download results and reports

## Project Structure

- **app.py**: Main Flask application entry point
- **Parts_Upload.py**: Handles parts data upload and management
- **check_status.py**: Manages status checking and reporting
- **wsgi.py**: WSGI entry point for production deployment
- **config.py**: Configuration settings (not tracked in git)
- **templates/**: HTML templates for the web interface
- **Static Data/**: Contains matrix and direct feed data
- **AutomationProcesses/**: Contains automation scripts for various tasks

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd WDA_MONITOR
   ```

2. Create a virtual environment:
   ```
   python -m venv myenv
   ```

3. Activate the virtual environment:
   - Windows: `myenv\Scripts\activate`
   - Linux/Mac: `source myenv/bin/activate`

4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

5. Create a `config.py` file with the following structure:
   ```python
   class Config:
       SECRET_KEY = 'your-secret-key'
       WORK_FOLDER = 'Data'
       result_path = 'results'
       DB_URI = 'your-database-connection-string'
       INSTANT_CLIENT = 'instantclient_23_6'
       Date_to_expire = 7  # days
       shared_path = 'logs'
       # Add other configuration settings as needed
   ```

6. Create required directories:
   ```
   mkdir Data
   mkdir results
   mkdir logs
   mkdir automate_records
   ```

7. Download Oracle Instant Client and place it in the `instantclient_23_6` directory

## Usage

### Development Mode

Run the application in development mode:
```
python app.py
```

### Production Mode

Run the application in production mode using Waitress:
```
run_production.bat
```
or
```
python wsgi.py
```

### Offline Tool

Run the offline tool for retrieving results:
```
RUN OFFLINE TOOL.bat
```

## Web Interface

- **Home**: View and manage uploaded files
- **Visualizations**: Interactive charts and data analysis
- **Import Status**: Monitor import status of parts

## Scheduled Tasks

The application includes scheduled tasks:
- Daily status check at 1:00 AM
- Matrix download at 7:00 AM

## Database Structure

The application uses two main tables:
- **parts**: Stores part information including part number, manufacturer, module, and status
- **uploaded_files**: Tracks uploaded files with timestamps

## Requirements

See `requirements.txt` for a complete list of dependencies.

## License

[Your License Information]

## Contributors

[Your Contributors Information]
