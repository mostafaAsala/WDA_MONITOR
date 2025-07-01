import os
import re

import numpy as np
print("import os")
import logging
print("import logging")
import pandas as pd
print("import pandas as pd")
from logging.handlers import RotatingFileHandler
print("from logging.handlers import RotatingFileHandler")
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for, session
print("from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for, session")
from flask_apscheduler import APScheduler
print("from flask_apscheduler import APScheduler")
from sqlalchemy import text
print("from sqlalchemy import text")
from werkzeug.utils import secure_filename
print("from werkzeug.utils import secure_filename")
from datetime import datetime, timedelta
print("from datetime import datetime")
from functools import wraps
print("from functools import wraps")
import hashlib
print("import hashlib")
from Parts_Upload import main_upload_parts, main_delete_file
print("from Parts_Upload import main_upload_parts, main_delete_file")
from check_status import Get_status, Download_results, download_summary_from_database, get_status_statistics, daily_check_all, get_wda_reg_aggregated_data, download_wda_reg_system_data,create_db_engine,run_daily_summary

print("from check_status import Get_status, Download_results, get_status_statistics, daily_check_all")
from config import Config
print("from config import Config")
import threading
print("import threading")
import filelock
print("import filelock")
import traceback
print("import traceback")
import time
print("import time")
from AutomationProcesses.AmazonUpload import upload_file_to_amazon
print("from AutomationProcesses.AmazonUpload import upload_file_to_amazon")
from AutomationProcesses.download_matrix import download_matrix_toFile
print("from AutomationProcesses.download_matrix import download_matrix_toFile")
from flask import render_template, jsonify, request, send_file
from AutomationProcesses.imported_notImported import Download_autoImported, automate_process, calculate_import_status, check_and_download_missing_dates, extract_part_details, move_exported_files
import os
from datetime import datetime
import zipfile
import glob
import pickle

#os.environ["MODIN_ENGINE"] = "dask"
#import modin.pandas as pd
print("import modin.pandas as pd")
class SafeRotatingFileHandler(RotatingFileHandler):
	def doRollover(self):
		"""
		Do a rollover, as described in __init__().
		"""
		if self.stream:
			self.stream.close()
			self.stream = None
		if self.backupCount > 0:
			for i in range(self.backupCount - 1, 0, -1):
				sfn = self.rotation_filename("%s.%d" % (self.baseFilename, i))
				dfn = self.rotation_filename("%s.%d" % (self.baseFilename, i + 1))
				if os.path.exists(sfn):
					if os.path.exists(dfn):
						try:
							os.remove(dfn)
						except Exception:
							time.sleep(0.1)  # Give some time for file to be released
							try:
								os.remove(dfn)
							except Exception:
								pass
					try:
						os.rename(sfn, dfn)
					except Exception:
						time.sleep(0.1)  # Give some time for file to be released
						try:
							os.rename(sfn, dfn)
						except Exception:
							pass
			dfn = self.rotation_filename(self.baseFilename + ".1")
			if os.path.exists(dfn):
				try:
					os.remove(dfn)
				except Exception:
					time.sleep(0.1)  # Give some time for file to be released
					try:
						os.remove(dfn)
					except Exception:
						pass
			try:
				os.rename(self.baseFilename, dfn)
			except Exception:
				time.sleep(0.1)  # Give some time for file to be released
				try:
					os.rename(self.baseFilename, dfn)
				except Exception:
					pass
		try:
			self.stream = open(self.baseFilename, 'w')
		except Exception:
			time.sleep(0.1)  # Give some time for file to be released
			self.stream = open(self.baseFilename, 'w')

# Add locks for critical sections
status_lock = threading.Lock()
file_lock = filelock.FileLock(os.path.join(Config.result_path, "results.csv.lock"))
df_lock = threading.Lock()
amazon_upload_lock = threading.Lock()
matrix_df = pd.read_csv(r'Static Data\matrix.csv')
direct_feed = pd.read_csv(r"Static Data\DFLIST+a 2.csv")

amazon_upload_in_progress = False
print("Done importing...")
# Add Oracle Client to PATH
os.environ["PATH"] = os.path.join(os.path.dirname(__file__), Config.INSTANT_CLIENT) + ";" + os.environ["PATH"]
# Global DataFrames
global_df = None
grouped_data = None
filtered_data = None

# WDA_Reg system data cache
wda_reg_system_data = pd.DataFrame()
wda_reg_data_lock = threading.Lock()
wda_reg_data_loaded = False
all_cs_labels = set()

def load_module_data():
	#load data from matrix man- module- running status - comment - old
	#load direct Feed suppliers
	#load man module table
	pass

def load_wda_reg_system_data():
	"""Load WDA_Reg system data into memory for fast access"""
	global wda_reg_system_data, wda_reg_data_loaded, all_cs_labels

	try:
		with wda_reg_data_lock:
			system_monitor_dir = os.path.join(os.getcwd(), "system_Monitor")
			latest_filepath = os.path.join(system_monitor_dir, "wda_reg_system_data_latest.csv")

			if os.path.exists(latest_filepath):
				# Load data from cached file
				wda_reg_system_data = pd.read_csv(latest_filepath,low_memory=False)

				# Convert date columns back to datetime
				date_columns = ['LRD2', 'V_NOTFOUND_DAT2', 'LR_DATE', 'download_timestamp']
				for col in date_columns:
					if col in wda_reg_system_data.columns:
						wda_reg_system_data[col] = pd.to_datetime(wda_reg_system_data[col], errors='coerce')
				print(wda_reg_system_data.columns)
				wda_reg_system_data['MAN_NAME'] = wda_reg_system_data['MAN_NAME'].astype(str)
				wda_reg_system_data['MODULE_NAME'] = wda_reg_system_data['MODULE_NAME'].astype(str)
				wda_reg_system_data['PRTY'] = wda_reg_system_data['PRTY'].astype(str)
				wda_reg_system_data['STATUS'] = wda_reg_system_data['STATUS'].astype(str)
				wda_reg_system_data['COUNT'] = wda_reg_system_data['COUNT'].astype(int)
				wda_reg_system_data['CS'] = wda_reg_system_data['CS'].astype(str)
				wda_reg_system_data['is_expired'] = wda_reg_system_data['is_expired'].astype(bool)
				wda_reg_system_data['date_only'] = wda_reg_system_data['LR_DATE'].dt.date
                    

                # Clean CS
				wda_reg_system_data['CS_clean'] = wda_reg_system_data['CS'].fillna('').astype(str).replace('nan', '')
                # Split and extract unique labels
				all_cs_labels = set()

				def clean_split(cs):
					if not cs:
						return []
					labels = [label.strip() for label in cs.split('|') if label.strip()]
					all_cs_labels.update(labels)
					return labels
                
				wda_reg_system_data['CS_labels_list'] = wda_reg_system_data['CS_clean'].apply(clean_split)
				for label in all_cs_labels:
					col_name = f'CS_{label}'
					wda_reg_system_data[col_name] = wda_reg_system_data['CS_labels_list'].apply(lambda x: int(label in x))
				wda_reg_system_data = wda_reg_system_data.drop(['CS_labels_list'], axis=1)
                        
				wda_reg_system_data['WDA_FLAG'] = wda_reg_system_data['WDA_FLAG'].astype(int)
				wda_reg_system_data['LC_OUTDATED'] = wda_reg_system_data['LC_OUTDATED'].astype(int)

				# Convert boolean column
				if 'is_expired' in wda_reg_system_data.columns:
					wda_reg_system_data['is_expired'] = wda_reg_system_data['is_expired'].astype(bool)

				wda_reg_data_loaded = True
				print(f"WDA_Reg system data loaded into memory: {len(wda_reg_system_data)} records")
				return True
			else:
				print("WDA_Reg system data file not found, will be created on first access")
				wda_reg_data_loaded = False
				return False

	except Exception as e:
		print(f"Error loading WDA_Reg system data: {e}")
		wda_reg_data_loaded = False
		return False

def load_data():
	global global_df
	global grouped_data
	try:
		global_df = pd.read_csv(os.path.join(Config.result_path, 'results.csv'),low_memory=False)
		global_df['status'] = global_df['status'].astype(str)
		global_df['status'].fillna('-',inplace=True)
		global_df['status_orig'] = global_df['status'].copy()
		global_df['status'] = global_df['status'].apply(lambda x:
			'WDA' if str(x) in ['Output Pattern not found','Link Step have no links','Error in loading page :404'] else
			'Not Found' if 'Not Found' in str(x) else
			'found' if 'found' in str(x) else
			'Proxy' if 'Error' in str(x) or 'Incomplete' in str(x) else
			'SW' if any(err in str(x) for err in [ 'Exception','java']) else
			'-' if '-' in str(x) else
			'not run' if str(x)=='' or  str(x)=='nan'  else
			'not assigned error'
		)

		global_df['upload_date'] = pd.to_datetime(global_df['upload_date'])
		global_df['last_run_date'] = pd.to_datetime(global_df['last_run_date'])

		global_df['done'] = global_df['last_run_date']>=(global_df['upload_date']- pd.Timedelta(days=1))

		global_df['prty'].fillna('-',inplace=True)
		global_df['table_name'].fillna('-',inplace=True)

		grouped_data = global_df.groupby(
    ['man', 'module', 'file_id', 'status', 'last_run_date',
     'table_name', 'prty', 'file_name', 'is_expired'], dropna=False
		).size().reset_index(name='count')
		# Rename the count column
		grouped_data.rename(columns={'id': 'count'}, inplace=True)

		grouped_data = global_df.groupby(
			['man', 'module', 'file_id', 'status', 'last_run_date',
			'table_name', 'prty', 'file_name', 'is_expired'], dropna=False
		).agg(
			count=('part', 'size'),  # Count the number of rows
			last_check_date=('last_check_date', 'first'),  # First value of last_check_date
			upload_date = ('upload_date','first'),
			stop_monitor_date=('stop_monitor_date', 'first'),  # First value of stop_monitor_date
			man_id=('man_id', 'first'),  # First value of man_id
			module_id=('module_id', 'first'),  # First value of module_id
			wda_flag=('wda_flag', 'first')  # First value of wda_flag
		).reset_index()

		grouped_data['upload_date'] = pd.to_datetime(grouped_data['upload_date'])
		grouped_data['last_run_date'] = pd.to_datetime(grouped_data['last_run_date'])

		grouped_data['done'] = grouped_data['last_run_date']>=(grouped_data['upload_date']- pd.Timedelta(days=1))
		print(grouped_data.columns)
		print(grouped_data)
		print("Length of data: ",len(global_df), len(grouped_data), grouped_data['count'].sum())

		# Also load WDA_Reg system data
		load_wda_reg_system_data()

		return True
	except Exception as e:
		print(f"Error loading data: {e}")
		return False

def create_app():
	app = Flask(__name__)
	app.config.from_object(Config)

	# Set secret key for sessions
	app.secret_key = getattr(Config, 'SECRET_KEY', 'your-secret-key-change-this')

	# Configure logging with the safe handler
	if not app.debug:
		if not os.path.exists('logs'):
			os.mkdir('logs')
		file_handler = SafeRotatingFileHandler(
			'logs/wda_monitor.log',
			maxBytes=10240,
			backupCount=10,
			delay=True  # Delay file creation until first write
		)
		file_handler.setFormatter(logging.Formatter(
			'%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
		))
		file_handler.setLevel(logging.INFO)
		app.logger.addHandler(file_handler)
		app.logger.setLevel(logging.INFO)
		app.logger.info('WDA Monitor startup')

	# Ensure work folders exist
	os.makedirs(Config.WORK_FOLDER, exist_ok=True)
	os.makedirs(Config.result_path, exist_ok=True)

	# Load initial data
	load_data()
	return app

app = create_app()

# Global dictionary to track file processing status
file_status = {}

# Global dictionary to track files currently being checked (in-memory only)
files_being_checked = set()
files_being_checked_lock = threading.Lock()

# User management system
USERS = {
    'WDA': {
        'password': hashlib.sha256('admin'.encode()).hexdigest(),
        'role': 'admin',
        'full_name': 'WDA Administrator',
        'permissions': ['all']
    },
	'Nader': {
        'password': hashlib.sha256('admin'.encode()).hexdigest(),
        'role': 'admin',
        'full_name': 'WDA Administrator',
        'permissions': ['all']
    },
	'Heba': {
        'password': hashlib.sha256('admin'.encode()).hexdigest(),
        'role': 'admin',
        'full_name': 'WDA Administrator',
        'permissions': ['all']
    },
	'Abdrabu': {
        'password': hashlib.sha256('admin'.encode()).hexdigest(),
        'role': 'admin',
        'full_name': 'WDA Administrator',
        'permissions': ['all']
    },
	'Mostafa': {
        'password': hashlib.sha256('admin'.encode()).hexdigest(),
        'role': 'admin',
        'full_name': 'WDA Administrator',
        'permissions': ['all']
    },

    'analyst1': {
        'password': hashlib.sha256('analyst123'.encode()).hexdigest(),
        'role': 'analyst',
        'full_name': 'Data Analyst 1',
        'permissions': ['view', 'check_status', 'download']
    },
    'analyst2': {
        'password': hashlib.sha256('analyst456'.encode()).hexdigest(),
        'role': 'analyst',
        'full_name': 'Data Analyst 2',
        'permissions': ['view', 'check_status', 'download']
    },
    'operator1': {
        'password': hashlib.sha256('operator123'.encode()).hexdigest(),
        'role': 'operator',
        'full_name': 'System Operator 1',
        'permissions': ['view', 'check_status', 'download', 'upload', 'delete']
    },
    'operator2': {
        'password': hashlib.sha256('operator456'.encode()).hexdigest(),
        'role': 'operator',
        'full_name': 'System Operator 2',
        'permissions': ['view', 'check_status', 'download', 'upload', 'delete']
    },
    'viewer1': {
        'password': hashlib.sha256('viewer123'.encode()).hexdigest(),
        'role': 'viewer',
        'full_name': 'Read Only User 1',
        'permissions': ['view']
    },
    'manager1': {
        'password': hashlib.sha256('manager123'.encode()).hexdigest(),
        'role': 'manager',
        'full_name': 'Department Manager',
        'permissions': ['view', 'check_status', 'download', 'upload', 'amazon_upload']
    }
}

# User activity logging
user_activity_log = []
user_activity_lock = threading.Lock()

def log_user_activity(username, action, details=None, ip_address=None):
    """Log user activity with timestamp"""
    with user_activity_lock:
        activity = {
            'timestamp': datetime.now(),
            'username': username,
            'action': action,
            'details': details or '',
            'ip_address': ip_address or request.remote_addr if request else 'Unknown'
        }
        user_activity_log.append(activity)

        # Keep only last 1000 entries to prevent memory issues
        if len(user_activity_log) > 1000:
            user_activity_log.pop(0)

def authenticate_user(username, password):
    """Authenticate user credentials"""
    if username in USERS:
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        if USERS[username]['password'] == hashed_password:
            return True
    return False

def get_user_info(username):
    """Get user information"""
    return USERS.get(username, None)

def has_permission(username, permission):
    """Check if user has specific permission"""
    user = get_user_info(username)
    if not user:
        return False
    return 'all' in user['permissions'] or permission in user['permissions']

def login_required(f):
    """Decorator to require login for routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            # Check if this is an AJAX/API request
            if request.is_json or request.headers.get('Content-Type') == 'application/json':
                return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
            else:
                # For regular page requests, redirect to login
                from flask import redirect, url_for
                return redirect('/login')
        return f(*args, **kwargs)
    return decorated_function

def permission_required(permission):
    """Decorator to require specific permission"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'username' not in session:
                # Check if this is an AJAX/API request
                if request.is_json or request.headers.get('Content-Type') == 'application/json':
                    return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
                else:
                    # For regular page requests, redirect to login
                    from flask import redirect
                    return redirect('/login')

            username = session['username']
            if not has_permission(username, permission):
                log_user_activity(username, 'PERMISSION_DENIED', f'Attempted to access {permission}')
                if request.is_json or request.headers.get('Content-Type') == 'application/json':
                    return jsonify({'error': 'Insufficient permissions'}), 403
                else:
                    # For regular page requests, redirect to login with error message
                    from flask import redirect
                    return redirect('/login?error=insufficient_permissions')

            return f(*args, **kwargs)
        return decorated_function
    return decorator

def admin_required(f):
    """Decorator to require admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            # Check if this is an AJAX/API request
            if request.is_json or request.headers.get('Content-Type') == 'application/json':
                return jsonify({'error': 'Authentication required', 'redirect': '/login'}), 401
            else:
                # For regular page requests, redirect to login
                from flask import redirect
                return redirect('/login')

        username = session['username']
        user = get_user_info(username)
        if not user or user['role'] != 'admin':
            log_user_activity(username, 'ADMIN_ACCESS_DENIED', 'Attempted to access admin-only resource')
            if request.is_json or request.headers.get('Content-Type') == 'application/json':
                return jsonify({'error': 'Admin access required'}), 403
            else:
                # For regular page requests, redirect to login with error message
                from flask import redirect
                return redirect('/login?error=admin_required')

        return f(*args, **kwargs)
    return decorated_function

def is_file_being_checked(file_name):
    """Check if a file is currently being checked"""
    with files_being_checked_lock:
        return file_name in files_being_checked

def add_file_to_checking(file_name):
    """Add a file to the checking set"""
    with files_being_checked_lock:
        files_being_checked.add(file_name)

def remove_file_from_checking(file_name):
    """Remove a file from the checking set"""
    with files_being_checked_lock:
        files_being_checked.discard(file_name)

def get_files_being_checked():
    """Get a copy of files currently being checked"""
    with files_being_checked_lock:
        return files_being_checked.copy()

# Path to store scheduled files
SCHEDULED_FILES_PATH = os.path.join(Config.result_path, 'scheduled_files.pkl')

# Load scheduled files if exists
scheduled_files = []
if os.path.exists(SCHEDULED_FILES_PATH):
    try:
        with open(SCHEDULED_FILES_PATH, 'rb') as f:
            scheduled_files = pickle.load(f)
    except Exception as e:
        app.logger.error(f'Error loading scheduled files: {str(e)}')

# Authentication routes
@app.route('/login', methods=['GET', 'POST'])
def login():
	if request.method == 'GET':
		return render_template('login.html')

	data = request.get_json()
	username = data.get('username')
	password = data.get('password')

	if authenticate_user(username, password):
		session['username'] = username
		user_info = get_user_info(username)
		session['role'] = user_info['role']
		session['full_name'] = user_info['full_name']

		log_user_activity(username, 'LOGIN', 'User logged in successfully')

		return jsonify({
			'status': 'success',
			'message': 'Login successful',
			'user': {
				'username': username,
				'role': user_info['role'],
				'full_name': user_info['full_name'],
				'permissions': user_info['permissions']
			}
		})
	else:
		log_user_activity(username or 'Unknown', 'LOGIN_FAILED', 'Invalid credentials')
		return jsonify({'status': 'error', 'message': 'Invalid credentials'}), 401

@app.route('/logout', methods=['POST'])
@login_required
def logout():
	username = session.get('username')
	log_user_activity(username, 'LOGOUT', 'User logged out')
	session.clear()
	return jsonify({'status': 'success', 'message': 'Logged out successfully'})

@app.route('/check-auth', methods=['GET'])
def check_auth():
	if 'username' in session:
		user_info = get_user_info(session['username'])
		return jsonify({
			'authenticated': True,
			'user': {
				'username': session['username'],
				'role': session.get('role'),
				'full_name': session.get('full_name'),
				'permissions': user_info['permissions'] if user_info else []
			}
		})
	return jsonify({'authenticated': False})

@app.route('/get-file-status', methods=['GET'])
@login_required
def get_file_status():
	return jsonify(file_status)

@app.route('/admin/dashboard')
@admin_required
def admin_dashboard():
	"""Admin dashboard page"""
	log_user_activity(session['username'], 'ADMIN_DASHBOARD_ACCESS', 'Accessed admin dashboard')
	return render_template('admin_dashboard.html')

@app.route('/admin/user-logs')
@admin_required
def get_user_logs():
	"""Get user activity logs for admin"""
	with user_activity_lock:
		logs = user_activity_log.copy()

	# Convert datetime objects to strings for JSON serialization
	formatted_logs = []
	for log in reversed(logs):  # Show most recent first
		formatted_logs.append({
			'timestamp': log['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
			'username': log['username'],
			'action': log['action'],
			'details': log['details'],
			'ip_address': log['ip_address']
		})

	return jsonify({'logs': formatted_logs})

@app.route('/admin/users')
@admin_required
def get_users():
	"""Get list of all users for admin"""
	users_info = []
	for username, user_data in USERS.items():
		users_info.append({
			'username': username,
			'role': user_data['role'],
			'full_name': user_data['full_name'],
			'permissions': user_data['permissions']
		})

	log_user_activity(session['username'], 'ADMIN_USERS_VIEW', 'Viewed users list')
	return jsonify({'users': users_info})

@app.route('/get-files-being-checked', methods=['GET'])
@login_required
def get_files_being_checked_endpoint():
	"""Get list of files currently being checked"""
	return jsonify({'files_being_checked': list(get_files_being_checked())})

@app.route('/update-file-status', methods=['POST'])
def update_file_status():
	data = request.json
	file_name = data.get('file_name')
	status = data.get('status')
	if file_name:
		file_status[file_name] = status
	return jsonify({'status': 'success'})

@app.route('/reset-file-status', methods=['POST'])
def reset_file_status():
	data = request.json
	files = data.get('files', [])
	for file in files:
		if file in file_status:
			file_status[file] = 'Idle'
		# Also remove from checking set
		remove_file_from_checking(file)
	return jsonify({'status': 'success'})

@app.route('/')
@login_required
def index():
	username = session['username']
	app.logger.info(f'User {username} accessing index page')
	log_user_activity(username, 'PAGE_ACCESS', 'Accessed main dashboard')

	files = os.listdir(Config.WORK_FOLDER)
	today = datetime.now().date()

	# Get database files info
	try:
		from check_status import get_files
		db_files_df = get_files()

		print(db_files_df)
		if db_files_df is not None:
			db_files = db_files_df.to_dict('records')
		else:
			db_files = []
	except Exception as e:
		app.logger.error(f'Error getting database files: {str(e)}')
		db_files = []

	user_info = get_user_info(username)
	return render_template('index.html', files=files, db_files=db_files, today=today,
						 scheduled_files=scheduled_files, user=user_info)

@app.route('/update-data', methods=['GET'])
def update_data():
	print("entered")
	try:
		app.logger.info('Updating global data')
		success = load_data()
		if success:
			return jsonify({'status': 'success', 'message': 'Data updated successfully'})
		else:
			return jsonify({'status': 'error', 'message': 'Failed to update data'}), 500
	except Exception as e:
		app.logger.error(f'Error updating data: {str(e)}')
		return jsonify({'status': 'error', 'message': str(e)}), 500



@app.route('/visuals')
@login_required
@permission_required('view')
def visualizations():
	username = session['username']
	app.logger.info(f'User {username} accessing visualizations page')
	log_user_activity(username, 'PAGE_ACCESS', 'Accessed visualizations page')
	user_info = get_user_info(username)
	return render_template('visuals.html', user=user_info)

@app.route('/wda-reg-monitor')
@login_required
@permission_required('view')
def wda_reg_monitor():
	username = session['username']
	app.logger.info(f'User {username} accessing WDA_Reg Monitor page')
	log_user_activity(username, 'PAGE_ACCESS', 'Accessed WDA_Reg Monitor page')
	user_info = get_user_info(username)
	return render_template('wda_reg_monitor.html', user=user_info)

@app.route('/upload', methods=['POST'])
@login_required
@permission_required('upload')
def upload_file():
	username = session['username']
	print(request.files)
	if 'file' not in request.files:
		app.logger.error('No file part in request')
		return jsonify({'error': 'No file part'}), 400

	file = request.files['file']
	if file.filename == '':
		app.logger.error('No selected file')
		return jsonify({'error': 'No selected file'}), 400

	if file:
		filename = secure_filename(file.filename)
		base_name = os.path.splitext(filename)[0]
		date_str = datetime.now().date()
		new_filename = f"{base_name}@{date_str}.txt"
		file_path = os.path.join(Config.WORK_FOLDER, new_filename)

		try:
			file.save(file_path)
			app.logger.info(f'User {username} uploaded file: {new_filename}')
			log_user_activity(username, 'FILE_UPLOAD', f'Uploaded: {new_filename}')
			main_upload_parts(Config.WORK_FOLDER)
			app.logger.info('File processed successfully')
			return jsonify({'message': 'File uploaded and processed successfully'})
		except Exception as e:
			app.logger.error(f'Error processing file: {str(e)}')
			log_user_activity(username, 'FILE_UPLOAD_ERROR', f'Failed to upload: {filename} - {str(e)}')
			return jsonify({'error': str(e)}), 500

@app.route('/delete/<filename>', methods=['POST'])
@login_required
@permission_required('delete')
def delete_file(filename):
	username = session['username']
	try:
		app.logger.info(f'User {username} attempting to delete file: {filename}')
		log_user_activity(username, 'FILE_DELETE', f'Deleted: {filename}')
		main_delete_file(Config.WORK_FOLDER, filename)
		app.logger.info(f'File deleted successfully by {username}: {filename}')
		return jsonify({'message': 'File deleted successfully'})
	except Exception as e:
		app.logger.error(f'Error deleting file {filename} by {username}: {str(e)}')
		log_user_activity(username, 'FILE_DELETE_ERROR', f'Failed to delete: {filename} - {str(e)}')
		return jsonify({'error': str(e)}), 500


@app.route('/upload-to-amazon', methods=['POST'])
@login_required
@permission_required('amazon_upload')
def upload_to_amazon():
    global amazon_upload_in_progress
    username = session['username']
    filename = request.json.get('file_name')
    if not filename:
        return jsonify({'error': 'Filename is required'}), 400

    # Check if another upload is in progress
    if amazon_upload_in_progress:
        return jsonify({'error': 'Another upload to Amazon is in progress. Please wait.'}), 429

    try:
        with amazon_upload_lock:
            amazon_upload_in_progress = True
            app.logger.info(f'User {username} uploading file to Amazon: {filename}')
            log_user_activity(username, 'AMAZON_UPLOAD', f'Uploaded to Amazon: {filename}')
            file_path = os.path.join(Config.WORK_FOLDER, filename)
            df = pd.read_csv(file_path, sep='\t',low_memory=False)
            upload_file_to_amazon(df,filename)
            app.logger.info(f'File uploaded to Amazon successfully by {username}: {filename}')
            return jsonify({'message': 'File uploaded to Amazon successfully'})
    except Exception as e:
        app.logger.error(f'Error uploading file to Amazon by {username}: {str(e)}')
        log_user_activity(username, 'AMAZON_UPLOAD_ERROR', f'Failed to upload to Amazon: {filename} - {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        amazon_upload_in_progress = False


@app.route('/status', methods=['POST'])
@login_required
@permission_required('check_status')
def check_status():
	selected_files = request.json.get('files', [])
	ignore_date = request.json.get('ignore_date', True)
	username = session['username']

	try:
		app.logger.info(f'User {username} checking status for files: {selected_files}')
		log_user_activity(username, 'STATUS_CHECK', f'Files: {", ".join(selected_files)}')

		# Check for files already being checked and filter them out
		files_to_check = []
		files_already_checking = []
		print(selected_files, get_files_being_checked())
		for file in selected_files:
			if is_file_being_checked(file):
				files_already_checking.append(file)
				app.logger.warning(f'File {file} is already being checked, skipping')
			else:
				files_to_check.append(file)
				add_file_to_checking(file)
				file_status[file] = 'Checking Status'

		# If no files can be checked, return error message
		if not files_to_check:
			error_messages = [f"file {file} already in progress" for file in files_already_checking]
			return jsonify({
				'status': 'Error',
				'message': '; '.join(error_messages),
				'files_already_checking': files_already_checking
			}), 400

		# If some files are already being checked, inform the user
		if files_already_checking:
			app.logger.warning(f'Some files already being checked: {files_already_checking}')

		try:
			with status_lock:  # Ensure only one status check at a time
				files_string, daily_export = Get_status(files_to_check, ignore_date)
				df, file_name  = Download_results(files_string, daily_export)
				# Use file lock when writing results
				with file_lock:
					status_stats = get_status_statistics(df)

				# Reset status to Idle and remove from checking set
				for file in files_to_check:
					file_status[file] = 'Idle'
					remove_file_from_checking(file)

				app.logger.info('Status check completed successfully')

				message = 'Status updated successfully'
				if files_already_checking:
					message += f'. Note: Files already being checked were skipped: {", ".join(files_already_checking)}'

				return_object = jsonify({
					'status': 'Success',
					'message': message,
					'data': status_stats,
					'files_checked': files_to_check,
					'files_skipped': files_already_checking
				})

				return return_object
		except Exception as e:
			# Reset status on error and remove from checking set
			for file in files_to_check:
				file_status[file] = 'Idle'
				remove_file_from_checking(file)
			raise e

	except Exception as e:
		app.logger.error(f'Error checking status: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/check-valid-parts', methods=['POST'])
def check_valid_parts():
	selected_files = request.json.get('files', [])

	try:
		app.logger.info(f'Checking valid parts for files: {selected_files}')

		with status_lock:  # Ensure only one check at a time
			from check_status import check_valid_file
			result = check_valid_file(selected_files)

			app.logger.info('Valid parts check completed successfully')
			return jsonify({
				'status': 'Success',
				'message': 'Valid parts check completed successfully',
				'data': result
			})

	except Exception as e:
		app.logger.error(f'Error checking valid parts: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/refresh-files', methods=['GET'])
def refresh_files():
	try:
		app.logger.info('Refreshing files list')
		files = os.listdir(Config.WORK_FOLDER)

		files_status = jsonify({
			'status': 'Success',
			'files': files
		})

		return files_status
	except Exception as e:
		app.logger.error(f'Error refreshing files: {str(e)}')
		return jsonify({'error': str(e)}), 500


@app.route('/get-scheduled-files', methods=['GET'])
def get_scheduled_files():
	try:
		app.logger.info('Getting scheduled files')
		return jsonify({
			'status': 'success',
			'files': scheduled_files
		})
	except Exception as e:
		app.logger.error(f'Error getting scheduled files: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/update-schedule-upload', methods=['POST'])
def update_schedule_upload():
	global scheduled_files
	try:
		data = request.json
		file_name = data.get('file_name')
		scheduled = data.get('scheduled')

		if not file_name:
			return jsonify({'status': 'error', 'error': 'File name is required'}), 400

		app.logger.info(f'{"Adding" if scheduled else "Removing"} {file_name} from scheduled uploads')

		if scheduled and file_name not in scheduled_files:
			scheduled_files.append(file_name)
		elif not scheduled and file_name in scheduled_files:
			scheduled_files.remove(file_name)

		# Save updated scheduled files
		with open(SCHEDULED_FILES_PATH, 'wb') as f:
			pickle.dump(scheduled_files, f)

		return jsonify({
			'status': 'success',
			'message': f'Successfully {"scheduled" if scheduled else "unscheduled"} {file_name}'
		})
	except Exception as e:
		app.logger.error(f'Error updating scheduled uploads: {str(e)}')
		return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/download', methods=['POST'])
def download_results():
	selected_files = request.json.get('files', [])
	try:
		app.logger.info(f'Downloading results for files: {selected_files}')
		df, filename = Download_results(selected_files)
		result_file = os.path.join(Config.result_path, filename)
		df.to_csv(result_file, index=False)
		app.logger.info(f'Results saved to: {result_file}')
		return send_file(result_file, as_attachment=True)
	except Exception as e:
		app.logger.error(f'Error downloading results: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/results.csv')
def serve_results():
	try:
		app.logger.info('Serving results.csv')
		result_file = os.path.join(Config.result_path, 'results.csv')
		return send_file(result_file, mimetype='text/csv')
	except Exception as e:
		app.logger.error(f'Error serving results.csv: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/api/filter-options')
def get_filter_options():
	try:
		app.logger.info('Fetching filter options')
		if grouped_data is None:
			print("--------------------------------")
			print("loading grouped data")
			load_data()
		print("Loading data ...")
		filers_dict = {
			'teams': sorted(grouped_data['module'].unique().tolist()),
			'status': sorted(grouped_data['status'].unique().tolist()),
			'Files': sorted(grouped_data['file_name'].unique().tolist()),
			'projects': sorted(grouped_data['file_name'].unique().tolist()),
			'manufacturers': sorted(grouped_data[grouped_data['man'].notna()]['man'].unique().tolist()),
			'priorities': sorted(grouped_data['prty'].astype(str).unique().tolist()),
			'table_name': sorted(grouped_data[grouped_data['table_name'].notna()]['table_name'].unique().tolist()),
			'running_status': ['Stopped', 'Regular Running', 'Run By Request', 'schedule Running']
		}
		filter_options = jsonify(filers_dict)
		print(filter_options)
		return filter_options

	except Exception as e:
		app.logger.error(f'Error fetching filter options: {str(traceback.format_exc())}')

		return jsonify({'error': str(e)}), 500

@app.route('/api/chart-data', methods=['GET','POST'])
def get_chart_data():
	global filtered_data
	try:
		app.logger.info('Fetching chart data')
		if grouped_data is None:
			load_data()

		df = grouped_data.copy()

		# Apply filters
		filters = request.json
		print(filters)
		if filters.get('module') and len(filters.get('module'))>0 :
			df = df[df['module'].isin(filters['module'])]
		if filters.get('file_name') and len(filters.get('file_name'))>0:
			df = df[df['file_name'].isin(filters['file_name'])]
		if filters.get('man') and len(filters.get('man'))>0:
			df = df[df['man'].isin(filters['man'])]
		if filters.get('status') and len(filters.get('status'))>0:
			stfilter =  filters['status']
			df = df[df['status'].isin(stfilter) ]
		if filters.get('prty') and len(filters.get('prty'))>0:
			ptfilters = filters['prty']
			df = df[df['prty'].isin(ptfilters) ]
		if filters.get('is_expired') and len(filters.get('is_expired'))>0:
			df = df[df['is_expired'].isin([x.lower() == 'true' for x in filters['is_expired']])]
		if filters.get('table_name') and len(filters.get('table_name'))>0:
			df = df[df['table_name'].isin(filters['table_name'])]
		if filters.get('issue_modules') and len(filters.get('issue_modules'))>0:
			df = df[df['issue_modules'].isin([x.lower() == 'true' for x in filters['issue_modules']])]
		if filters.get('startDate') and filters.get('endDate') :
			df['last_run_date'] = pd.to_datetime(df['last_run_date'])
			df = df[(df['last_run_date'] >= filters['startDate']) &
				   (df['last_run_date'] <= filters['endDate'])]
		if filters.get('done') and len(filters.get('done'))>0:
			df = df[df['done'].isin([int(x) for x in filters['done']])]
		if filters.get('running_status') and len(filters.get('running_status'))>0:
			# Convert wda_flag to running status string
			number_to_string = {
				0: "Stopped",
				1: "Regular Running",
				2: "Run By Request",
				3: "schedule Running",
				4: "schedule Running"
			}

			df['running_status'] = df['wda_flag'].map(lambda x: number_to_string.get(int(x), "Unknown"))

			df = df[df['running_status'].isin(filters['running_status'])]
		if filters.get('direct_feed') and len(filters.get('direct_feed'))>0:
			# Get direct feed status from the module_stats calculation
			df_with_direct_feed = df.copy()
			df_with_direct_feed['direct_feed'] = df_with_direct_feed.apply(
				lambda row: 1 if row['man'] in direct_feed[direct_feed['Direct Feed'] == 1]['Supplier'].values else 0,
				axis=1
			)
			df = df_with_direct_feed[df_with_direct_feed['direct_feed'].isin([int(x) for x in filters['direct_feed']])]
		filtered_data = df
		# Calculate statistics using count column and convert to native Python types
		stats = {
			'totalParts': int(df['count'].sum()),
			'expiredParts': int(df[df['is_expired'] == True]['count'].sum()),
			'activeParts': int(df[df['is_expired'] == False]['count'].sum()),
			'errorParts': int(df[df['status'].isin(['Error', 'Proxy'])]['count'].sum()),
			'missingParts': int(df[df['table_name'] == 'not run']['count'].sum()),
			'moduleCount': len(df['module'].unique())
		}

		# Prepare chart data with modified status and convert to native Python types
		status_counts = df.groupby('status')['count'].sum()
		module_counts = df.groupby('module')['count'].sum()
		table_name_counts = df.groupby('table_name')['count'].sum()

		# Add timeline data
		df['last_run_date'] = pd.to_datetime(df['last_run_date'])
		timeline_data = df.groupby('last_run_date')['count'].sum().reset_index()
		timeline_data = timeline_data.sort_values('last_run_date')

		# Calculate module statistics
		module_stats = []
		current_date = pd.Timestamp.now()
		three_days_ago = current_date - pd.Timedelta(days=3)
		unique_pairs = df[['module', 'man']].drop_duplicates()

		for module, man in unique_pairs.itertuples(index=False):
			module_df = df[df['module'] == module]
			total_count = module_df['count'].sum()
			error_count = module_df[module_df['status'].isin(['Error', 'Proxy'])]['count'].sum()
			found_count = module_df[module_df['status'] == 'found']['count'].sum()
			expired_count = module_df[module_df['is_expired'] == True]['count'].sum()
			WDA_Flag = module_df['wda_flag'].iloc[0]
			number_to_string = {
				0: "Stopped",
				1: "Regular Running",
				2: "Run By Request",
				3: "schedule Running",
				4: 'schedule Running'
				# Add more mappings as needed
			}

			running_status = number_to_string[int(module_df['wda_flag'].iloc[0])]
			if module in matrix_df['Modules'].values:
				matrix_status = str(matrix_df[matrix_df['Modules'] == module]['Running Status'].iloc[0]).replace('"', '')
				if  matrix_status == 'nan':
					matrix_status = '-'
				matrix_comment = str(matrix_df[matrix_df['Modules'] == module]['Module Comment'].iloc[0]).replace('"', '')
				if  matrix_comment== 'nan':
					matrix_comment = '-'
				matrix_old = str(matrix_df[matrix_df['Modules'] == module]['old'].iloc[0]).replace('"', '')
				if  matrix_old == 'nan':
					matrix_old = '-'
				DFF = direct_feed[direct_feed['Supplier'] == man]['Direct Feed']
				if len(DFF) <1:
					direct_feed_status = 0
				else:
					direct_feed_status =int(DFF.iloc[0])
			else:
				matrix_status = '-'
				matrix_comment = '-'
				matrix_old = '-'
				direct_feed_status = 0


			#.map({0:'Stopped', 1:'Regular Running',2:'Run By Request',3:'schedule Running'})
			# Calculate last 3 days statistics
			recent_df = module_df[module_df['last_run_date'] >= three_days_ago]
			recent_count = recent_df['count'].sum()
			recent_percentage = round((recent_count / total_count) * 100, 2) if total_count > 0 else 0

			module_stats.append({
				'module': module,
				'running_status': running_status,
				'matrix_status': matrix_status,
				'matrix_comment': matrix_comment,
				'matrix_old': matrix_old,
				'direct_feed_status': direct_feed_status,
				'total_count': int(total_count),
				'error_count': int(error_count),
				'error_percentage': round((error_count / total_count) * 100, 2) if total_count > 0 else 0,
				'found_count': int(found_count),
				'found_percentage': round((found_count / total_count) * 100, 2) if total_count > 0 else 0,
				'expired_count': int(expired_count),
				'expired_percentage': round((expired_count / total_count) * 100, 2) if total_count > 0 else 0,
				'recent_count': int(recent_count),
				'recent_percentage': recent_percentage
			})

		# Sort by total count descending
		module_stats.sort(key=lambda x: x['total_count'], reverse=True)
		# Calculate file statistics
		print("Calculating file statistics")
		file_stats = []
		for file_name in df['file_name'].unique():
			file_df = df[df['file_name'] == file_name]
			total_count = file_df['count'].sum()
			error_count = file_df[file_df['status'].isin(['Error','Exception', 'Proxy','Incomplete'])]['count'].sum()
			notFound_count = file_df[file_df['status']== 'Not Found']['count'].sum()
			found_count = file_df[file_df['status'] == 'found']['count'].sum()
			Done_parts = file_df[file_df['last_run_date']>=(file_df['upload_date']- pd.Timedelta(days=1))]['count'].sum()
			error_Done_parts = file_df[(file_df['last_run_date']>=file_df['upload_date']) & file_df['status'].isin(['Error','Exception', 'Proxy','Incomplete'])]['count'].sum()
			file_stats.append({
				'file': file_name,
				'total_count': int(total_count),
				'error_count': int(error_count),
				'NotFound_count': int(notFound_count),
				'error_percentage': round((error_count / total_count) * 100, 2) if total_count > 0 else 0,
				'found_count': int(found_count),
				'found_percentage': round((found_count / total_count) * 100, 2) if total_count > 0 else 0,
				'done_percentage': round((Done_parts / total_count) * 100, 2) if total_count > 0 else 0,
				'Error_done_percentage': round((error_Done_parts / Done_parts) * 100, 2) if Done_parts > 0 else 0
			})

		# Sort by total count descending
		file_stats.sort(key=lambda x: x['total_count'], reverse=True)

		# Calculate new validation statistics
		validation_stats = {
			'stopped_modules': {
				'count': len([m for m in module_stats if m['running_status'] == 'Stopped']),
				'percentage': round(len([m for m in module_stats if m['running_status'] == 'Stopped']) / len(module_stats) * 100, 2)
			},
			'stopped_parts': {
				'count': sum(m['total_count'] for m in module_stats if m['running_status'] == 'Stopped'),
				'percentage': round(sum(m['total_count'] for m in module_stats if m['running_status'] == 'Stopped') / stats['totalParts'] * 100, 2)
			},
			'pending_modules': {
				'count': len([m for m in module_stats if m['matrix_status'] == 'Pending data team']),
				'percentage': round(len([m for m in module_stats if m['matrix_status'] == 'Pending data team']) / len(module_stats) * 100, 2)
			},
			'pending_parts': {
				'count': sum(m['total_count'] for m in module_stats if m['matrix_status'] == 'Pending data team'),
				'percentage': round(sum(m['total_count'] for m in module_stats if m['matrix_status'] == 'Pending data team') / stats['totalParts'] * 100, 2)
			},
			'direct_feed_modules': {
				'count': len([m for m in module_stats if m['direct_feed_status'] == 1]),
				'percentage': round(len([m for m in module_stats if m['direct_feed_status'] == 1]) / len(module_stats) * 100, 2)
			}
		}

		# Calculate total percentage for validation
		total_critical_percentage = (
			validation_stats['stopped_parts']['percentage'] +
			validation_stats['pending_parts']['percentage'] +
			validation_stats['direct_feed_modules']['percentage']
		)

		validation_stats['file_status'] = 'Rejected' if total_critical_percentage > 10 else 'Accepted'
		validation_stats['total_critical_percentage'] = round(total_critical_percentage, 2)

		# Modify the return_data to include validation stats
		return_data = jsonify({
			'stats': stats,
			'validation_stats': validation_stats,  # Add the new validation stats
			'tableData': module_stats,
			'fileStats': file_stats,
			'status': {
				'labels': status_counts.index.tolist(),
				'values': [int(x) for x in status_counts.values.tolist()]
			},
			'isExpired': {
				'labels': ['Expired', 'Not Expired'],
				'values': [int(stats['expiredParts']), int(stats['activeParts'])]
			},
			'team': {
				'labels': module_counts.index.tolist(),
				'values': [int(x) for x in module_counts.values.tolist()]
			},
			'table_name': {
				'labels': table_name_counts.index.tolist(),
				'values': [int(x) for x in table_name_counts.values.tolist()]
			},
			'timeline': {
				'dates': timeline_data['last_run_date'].dt.strftime('%Y-%m-%d').tolist(),
				'counts': [int(x) for x in timeline_data['count'].tolist()]
			}
		})

		print("Returning data")

		return return_data
	except Exception as e:
		app.logger.error(f'Error fetching chart data: {str(e)}')
		print(traceback.format_exc())
		return jsonify({'error': str(e)}), 500



def Get_filtered_data(df, filters):
	# Apply existing filters first
	if filters.get('module'):
		df = df[df['module'].isin(filters['module'])]
	if filters.get('file_name'):
		df = df[df['file_name'].isin(filters['file_name'])]
	if filters.get('man'):
		df = df[df['man'].isin(filters['man'])]
	if filters.get('status'):
		df['status_category'] = df['status'].apply(lambda x:
			'Proxy' if '403' in str(x) else
			'Error' if any(err in str(x) for err in ['Error', 'Exception', 'Incomplete']) else
			x
		)
		df = df[df['status_category'].isin(filters['status'])]
	if filters.get('prty'):
		df = df[df['prty'].isin(filters['prty'])]
	if filters.get('is_expired'):
		df = df[df['is_expired'].isin([x.lower() == 'true' for x in filters['is_expired']])]
	if filters.get('table_name'):
		df = df[df['table_name'].isin(filters['table_name'])]
	if filters.get('issue_modules'):
		df = df[df['issue_modules'].isin([x.lower() == 'true' for x in filters['issue_modules']])]
	if filters.get('startDate') and filters.get('endDate'):
		df['last_run_date'] = pd.to_datetime(df['last_run_date'])
		df = df[(df['last_run_date'] >= filters['startDate']) &
				(df['last_run_date'] <= filters['endDate'])]
	if filters.get('done'):
		df = df[df['done'].isin([int(x) for x in filters['done']])]
	if filters.get('running_status'):
		# Convert wda_flag to running status string
		number_to_string = {
			0: "Stopped",
			1: "Regular Running",
			2: "Run By Request",
			3: "schedule Running",
			4: "schedule Running"
		}
		df['running_status'] = df['wda_flag'].map(lambda x: number_to_string.get(int(x), "Unknown"))
		print(df['running_status'].unique())
		print(filters['running_status'])
		df = df[df['running_status'].isin(filters['running_status'])]
	if filters.get('direct_feed'):
		# Get direct feed status from the module_stats calculation
		df_with_direct_feed = df.copy()
		df_with_direct_feed['direct_feed'] = df_with_direct_feed.apply(
			lambda row: 1 if row['man'] in direct_feed[direct_feed['Direct Feed'] == 1]['Supplier'].values else 0,
			axis=1
		)
		df = df_with_direct_feed[df_with_direct_feed['direct_feed'].isin([int(x) for x in filters['direct_feed']])]

	# Apply index range filter last
	if filters.get('startIndex') is not None and filters.get('endIndex') is not None:
		start_idx = int(filters['startIndex'])
		end_idx = int(filters['endIndex'])
		print(start_idx, end_idx)
		if start_idx <= end_idx and start_idx >= 0:# and end_idx <= len(df):
			df = df.iloc[start_idx:end_idx + 1]  # +1 because slice is exclusive of end
			print("filtered")

	return df

@app.route('/api/download-filtered', methods=['POST'])
def download_filtered():
	try:
		app.logger.info('Downloading filtered results')
		filters = request.json
		df = global_df.copy()
		df = Get_filtered_data(df, filters)

		"""# Apply filters
		if filters.get('module'):
			df = df[df['module'].isin(filters['module'])]
		if filters.get('file_name'):
			df = df[df['file_name'].isin(filters['file_name'])]
		if filters.get('man'):
			df = df[df['man'].isin(filters['man'])]
		if filters.get('status'):
			# Modify status categorization before applying filter
			df['status_category'] = df['status'].apply(lambda x:
				'Proxy' if '403' in str(x) else
				'Error' if any(err in str(x) for err in ['Error', 'Exception', 'Incomplete']) else
				x
			)
			df = df[df['status_category'].isin(filters['status'])]
		if filters.get('prty'):
			df = df[df['prty'].isin(filters['prty'])]
		if filters.get('is_expired'):
			df = df[df['is_expired'].isin([x.lower() == 'true' for x in filters['is_expired']])]
		if filters.get('table_name'):
			df = df[df['table_name'].isin(filters['table_name'])]
		if filters.get('issue_modules'):
			df = df[df['issue_modules'].isin([x.lower() == 'true' for x in filters['issue_modules']])]
		if filters.get('startDate') and filters.get('endDate'):
			df['last_run_date'] = pd.to_datetime(df['last_run_date'])
			df = df[(df['last_run_date'] >= filters['startDate']) &
				   (df['last_run_date'] <= filters['endDate'])]
		if filters.get('done'):
			df = df[df['done'].isin([int(x) for x in filters['done']])]
		"""
		# Create a temporary file for the filtered results
		temp_file = os.path.join(Config.result_path, 'filtered_results.csv')
		df.to_csv(temp_file, index=False)

		return send_file(temp_file, as_attachment=True, download_name='filtered_results.csv')
	except Exception as e:
		app.logger.error(f'Error downloading filtered results: {str(e)}')
		return jsonify({'error': str(e)}), 500


@app.route('/update-stop-date', methods=['POST'])
def update_stop_date():
	try:
		app.logger.info('Updating stop monitor date')
		file_name = request.json.get('file_name')
		stop_date = request.json.get('stop_date')

		
		engine = create_db_engine()

		with engine.begin() as connection:
			update_query = text("""
				UPDATE uploaded_files
				SET stop_monitor_date = TO_DATE(:stop_date, 'YYYY-MM-DD')
				WHERE file_name = :file_name
			""")
			connection.execute(update_query, {"stop_date": stop_date, "file_name": file_name})

		return jsonify({'status': 'success', 'message': 'Stop monitor date updated successfully'})
	except Exception as e:
		app.logger.error(f'Error updating stop monitor date: {str(e)}')
		return jsonify({'error': str(e)}), 500






@app.route('/get-db-files')
def get_db_files():
	try:
		app.logger.info('Fetching files from database')
		from check_status import get_files
		files_df = get_files()
		if files_df is not None:
			# Convert datetime columns to string format
			for col in ['upload_date', 'last_check_date', 'stop_monitor_date']:
				if col in files_df.columns:
					files_df[col] = files_df[col].astype(str)
			return jsonify({
				'status': 'success',
				'files': files_df.to_dict('records')
			})
		else:
			return jsonify({'status': 'error', 'message': 'No files found'}), 404
	except Exception as e:
		app.logger.error(f'Error fetching files from database: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/get_status_by_date', methods=['GET'])
def get_status_by_date():
    date = request.args.get('date')
    granularity = request.args.get('grant', 'day')  # Default to 'day' if not provided

    if not date:
        return jsonify({'error': 'Date parameter is required'}), 400

    try:
        date = pd.to_datetime(date)  # Convert string to datetime
    except Exception as e:
        return jsonify({'error': 'Invalid date format'}), 400

    print(f"Fetching status counts for date: {date}")

    # Define date filtering based on granularity
    if granularity == 'day':
        filtered_df = filtered_data[filtered_data['last_run_date'].dt.date == date.date()]
    elif granularity == 'month':
        filtered_df = filtered_data[(filtered_data['last_run_date'] >= date) & (filtered_data['last_run_date'] < date + pd.DateOffset(months=1))]
    elif granularity == 'quarter':
        filtered_df = filtered_data[(filtered_data['last_run_date'] >= date) & (filtered_data['last_run_date'] < date + pd.DateOffset(months=3))]
    elif granularity == 'year':
        filtered_df = filtered_data[(filtered_data['last_run_date'] >= date) & (filtered_data['last_run_date'] < date + pd.DateOffset(years=1))]
    else:
        return jsonify({'error': 'Invalid granularity'}), 400

    if filtered_df.empty:
        return jsonify({"message": "No data found for this date"}), 404

    # Count occurrences of each status
    status_counts = filtered_df.groupby('status')['count'].sum().to_dict()
    print(status_counts)
    return jsonify({"date": date, "status_counts": status_counts})

def calculate_wda_reg_aggregations(df:pd.DataFrame):
    """Calculate pre-aggregated data for WDA_Reg visualizations"""
    global all_cs_labels
    try:
        time_start = time.time()
        # Basic statistics
        total_parts = int(df['COUNT'].sum())
        found_parts = int(df[df['STATUS'] == 'found']['COUNT'].sum())
        not_found_parts = int(df[df['STATUS'] == 'not found']['COUNT'].sum())
        not_run_parts = int(df[df['STATUS'] == 'not run']['COUNT'].sum())
        expired_parts = int(df[df['is_expired'] == True]['COUNT'].sum())
        expired_parts = int(df[df['PRTY'].str.startswith('P')]['COUNT'].sum())
        lc_outdated_parts = int(df[df['LC_OUTDATED'] == 1]['COUNT'].sum())
        unique_modules = int(df['MODULE_NAME'].nunique())
        unique_manufacturers = int(df['MAN_NAME'].nunique())
        
        # Calculate percentages
        found_percentage = round((found_parts / total_parts) * 100, 1) if total_parts > 0 else 0
        not_found_percentage = round((not_found_parts / total_parts) * 100, 1) if total_parts > 0 else 0
        not_run_percentage = round((not_run_parts / total_parts) * 100, 1) if total_parts > 0 else 0
        expired_percentage = round((expired_parts / total_parts) * 100, 1) if total_parts > 0 else 0
        lc_outdated_percentage = round((lc_outdated_parts / total_parts) * 100, 1) if total_parts > 0 else 0
        
        # Status distribution for charts
        status_counts = df.groupby('STATUS')['COUNT'].sum().to_dict()
        status_chart_data = {
            'labels': list(status_counts.keys()),
            'values': [int(v) for v in status_counts.values()],
            'colors': {
                'found': '#2ecc71',
                'not found': '#e74c3c',
                'not run': '#f39c12'
            }
        }

        # Priority distribution
        priority_counts = df.groupby('PRTY')['COUNT'].sum().to_dict()
        priority_chart_data = {
            'labels': sorted(priority_counts.keys()),
            'values': [int(priority_counts[k]) for k in sorted(priority_counts.keys())]
        }

        # Top manufacturers (top 10) - using MAN_NAME instead of MAN_ID
        manufacturer_counts = df.groupby('MAN_NAME')['COUNT'].sum().sort_values(ascending=False).head(10).to_dict()
        manufacturer_chart_data = {
            'labels': list(manufacturer_counts.keys()),
            'values': [int(v) for v in manufacturer_counts.values()]
        }

        # WDA Flag distribution
        wda_flag_counts = df.groupby('WDA_FLAG')['COUNT'].sum().to_dict()
        wda_flag_chart_data = {
            'labels': list(wda_flag_counts.keys()),
            'values': [int(v) for v in wda_flag_counts.values()],
            'colors': {
                'Y': '#28a745',  # Green for Yes
                'N': '#dc3545'   # Red for No
            }
        }

        # LC Outdated distribution
        lc_outdated_counts = df.groupby('LC_OUTDATED')['COUNT'].sum().to_dict()
        lc_outdated_chart_data = {
            'labels': ['Up to Date', 'Outdated'],
            'values': [int(lc_outdated_counts.get(0, 0)), int(lc_outdated_counts.get(1, 0))],
            'colors': {
                0: '#28a745',  # Green for Up to Date
                1: '#dc3545'   # Red for Outdated
            }
        }
        print("time for lc outdated chart: ",time.time()-time_start)
        time_start = time.time()

        # CS distribution (optimized multi-label processing)
        #get count of each cs label
        cs_chart_data = {
            'labels': list(all_cs_labels),
            'values': [int((df[f'CS_{label}'] * df['COUNT']).sum()) for label in all_cs_labels]
        }
            

        print("time for cs chart: ",time.time()-time_start)
        time_start = time.time()

        # Expired vs Active
        expired_chart_data = {
            'labels': ['Active', 'Expired'],
            'values': [total_parts - expired_parts, expired_parts],
            'colors': ['#27ae60', '#e67e22']
        }
        print("time for expired chart: ",time.time()-time_start)
        time_start = time.time()

        # Get summary table data for the summary timeline chart
        summary_timeline_data = get_summary()
        summary_timeline_data = summary_timeline_data.to_dict('records')
        #print("summary_timeline_data: ",summary_timeline_data)
        print("time for summary timeline data: ",time.time()-time_start)
        time_start = time.time()
        # Timeline data (by LR_DATE) - initially show past year
        timeline_data = {'dates': [], 'counts': [], 'date_range': {'min_date': '', 'max_date': ''}}
        if 'LR_DATE' in df.columns:
            df_timeline = df.dropna(subset=['LR_DATE'])

            if not df_timeline.empty:
                # Get date range for filter options
                min_date = df_timeline['LR_DATE'].min().date()
                max_date = df_timeline['LR_DATE'].max().date()

                # Default to past year for initial display
                one_year_ago = datetime.now().date() - timedelta(days=365)
                start_date = max(min_date, one_year_ago)  # Use the later of min_date or one year ago

                # Filter to past year by default
                df_timeline_filtered = df_timeline[df_timeline['LR_DATE'].dt.date >= start_date]

                if not df_timeline_filtered.empty:
                    timeline_counts = df_timeline_filtered.groupby('date_only')['COUNT'].sum().sort_index()
                    timeline_data = {
                        'dates': [str(d) for d in timeline_counts.index],
                        'counts': [int(v) for v in timeline_counts.values],
                        'date_range': {
                            'min_date': str(min_date),
                            'max_date': str(max_date),
                            'default_start': str(start_date),
                            'default_end': str(max_date)
                        }
                    }

        print("time for timeline data: ",time.time()-time_start)
        time_start = time.time()
        # Use the already extracted CS labels for filter options (reuse from chart calculation)
        cs_labels = all_cs_labels

        # Filter options for client-side filtering
        filter_options = {
            'man_names': sorted(df['MAN_NAME'].unique().tolist()),
            'module_names': sorted(df['MODULE_NAME'].unique().tolist()),
            'priorities': sorted(df['PRTY'].unique().tolist()),
            'statuses': sorted(df['STATUS'].unique().tolist()),
            'wda_flags': sorted(df['WDA_FLAG'].unique().tolist()),
            'lc_outdated_options': sorted(df['LC_OUTDATED'].unique().tolist()),
            'cs_labels': sorted(list(cs_labels)),
            'expired_options': ['true', 'false'],
            'error_status': sorted(df['ERROR_STATUS'].dropna().unique().tolist())  # <-- Add this line
        }
        print("time for filter options: ",time.time()-time_start)
        time_start = time.time()
        # Prepare aggregated table data (grouped by MAN_NAME and MODULE_NAME)
        table_data = []
        
        df['outdated'] = df['PRTY'].str.startswith('P') * df['COUNT']
        df['expired'] = (df['is_expired'] == True) * df['COUNT']
        df['found'] = (df['STATUS']=='found') * df['COUNT']  # Ensure STATUS has no NaN values
        df['notfound'] = (df['STATUS']=='not found') * df['COUNT']  # Ensure STATUS has no NaN values

        grouped_df = df.groupby(['MAN_NAME', 'MODULE_NAME']).agg({
        	'COUNT': 'sum',
        	'outdated': 'sum',
        	'expired': 'sum',
        	'found': 'sum',
        	'notfound': 'sum'
        }).reset_index()
        
        table_data = grouped_df.to_dict(orient='records')
        
        # Error Status distribution for charts
        error_status_counts = df.groupby('ERROR_STATUS')['COUNT'].sum().to_dict()
        error_status_chart_data = {
            'labels': list(error_status_counts.keys()),
            'values': [int(v) for v in error_status_counts.values()],
        }
        
        print("time for table data: ",time.time()-time_start)
        return {
            'stats': {
                'total_parts': total_parts,
                'found_parts': found_parts,
                'not_found_parts': not_found_parts,
                'not_run_parts': not_run_parts,
                'expired_parts': expired_parts,
                'lc_outdated_parts': lc_outdated_parts,
                'unique_modules': unique_modules,
                'unique_manufacturers': unique_manufacturers,
                'found_percentage': found_percentage,
                'not_found_percentage': not_found_percentage,
                'not_run_percentage': not_run_percentage,
                'expired_percentage': expired_percentage,
                'lc_outdated_percentage': lc_outdated_percentage
            },
            'charts': {
                'status': status_chart_data,
                'priority': priority_chart_data,
                'manufacturer': manufacturer_chart_data,
                'wda_flag': wda_flag_chart_data,
                'lc_outdated': lc_outdated_chart_data,
                'cs': cs_chart_data,
                'expired': expired_chart_data,
                'timeline': timeline_data,
                'summary_timeline': summary_timeline_data,
                'error_status': error_status_chart_data,  # <-- Add this line
            },
            'filter_options': filter_options,
            'table_data': table_data
        }

    except Exception as e:
        app.logger.error(f"Error calculating WDA_Reg aggregations: {str(e)}")
        app.logger.error(traceback.format_exc())
        return {
            'stats': {},
            'charts': {},
            'filter_options': {}
        }
@app.route('/api/wda-reg-data', methods=['GET'])
@login_required
@permission_required('view')
def get_wda_reg_data():
    """API endpoint to fetch WDA_Reg aggregated data with pre-calculated visualizations"""
    log_user_activity(session['username'], 'WDA_REG_DATA_ACCESS', 'Accessed WDA_Reg aggregated data from cache')
    username = session['username']
    try:
        app.logger.info(f'User {username} fetching WDA_Reg aggregated data from cache')
        log_user_activity(username, 'WDA_REG_DATA_ACCESS', 'Accessed WDA_Reg aggregated data from cache')

        global wda_reg_system_data, wda_reg_data_loaded

        # Check if data is loaded in memory
        if not wda_reg_data_loaded or wda_reg_system_data.empty:
            # Try to load data into memory
            if not load_wda_reg_system_data():
                # If loading fails, try to get fresh data from database
                app.logger.warning('Loading from cache failed, fetching fresh data from database')
                df = get_wda_reg_aggregated_data()
            else:
                df = wda_reg_system_data.copy()
        else:
            # Use cached data from memory
            df = wda_reg_system_data.copy()

        # Pre-calculate aggregated data for visualizations
        aggregated_data = calculate_wda_reg_aggregations(df)
        print("aggregated_data: ",aggregated_data)
        app.logger.info(f'Successfully served WDA_Reg aggregations from cache ({len(df)} records processed)')

        return jsonify({
            'status': 'success',
            'aggregations': aggregated_data,
            'total_records': len(df),
            'from_cache': wda_reg_data_loaded
        })

    except Exception as e:
        app.logger.error(f'Error fetching WDA_Reg data: {traceback.format_exc()}')
        log_user_activity(username, 'WDA_REG_DATA_ERROR', f'Error fetching WDA_Reg data: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/refresh-wda-reg-data', methods=['POST'])
@login_required
@permission_required('view')
def refresh_wda_reg_data():
    """API endpoint to manually refresh WDA_Reg system data"""
    username = session['username']
    try:
        app.logger.info(f'User {username} manually refreshing WDA_Reg system data')
        log_user_activity(username, 'WDA_REG_REFRESH', 'Manually refreshed WDA_Reg system data')

        # Force download new data
        filepath = download_wda_reg_system_data()

        # Reload data into memory cache
        load_wda_reg_system_data()

        app.logger.info(f'Successfully refreshed WDA_Reg system data to: {filepath} and reloaded into memory')

        return jsonify({
            'status': 'success',
            'message': 'WDA_Reg system data refreshed and reloaded into memory successfully',
            'filepath': filepath
        })

    except Exception as e:
        app.logger.error(f'Error refreshing WDA_Reg system data: {str(e)}')
        log_user_activity(username, 'WDA_REG_REFRESH_ERROR', f'Error refreshing WDA_Reg data: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/wda-reg-filtered-aggregations', methods=['POST'])
@login_required
@permission_required('view')
def get_wda_reg_filtered_aggregations():
    """API endpoint to get filtered aggregations for WDA_Reg data"""
    n_time = time.time()
    username = session['username']
    try:
        app.logger.info(f'User {username} requesting filtered WDA_Reg aggregations')
        log_user_activity(username, 'WDA_REG_FILTERED_ACCESS', 'Accessed filtered WDA_Reg aggregations')

        global wda_reg_system_data, wda_reg_data_loaded

        # Get data from memory cache
        if not wda_reg_data_loaded or wda_reg_system_data.empty:
            print("loading from database")
            if not load_wda_reg_system_data():
                df = get_wda_reg_aggregated_data()
            else:
                df = wda_reg_system_data.copy()
        else:
            print("loading from cache")
            df = wda_reg_system_data.copy()

        
        print("time for loading: ",time.time()-n_time)
        n_time = time.time()
        # Apply filters from request
        filters = request.json or {}
        filtered_df = apply_wda_reg_filters(df, filters)
        print("time for filtering: ",time.time()-n_time)
        n_time = time.time()
        # Calculate aggregations for filtered data
        aggregated_data = calculate_wda_reg_aggregations(filtered_df)
        print("time for aggregation: ",time.time()-n_time)
        app.logger.info(f'Successfully served filtered WDA_Reg aggregations ({len(filtered_df)} records after filtering)')

        return jsonify({
            'status': 'success',
            'aggregations': aggregated_data,
            'total_records': len(filtered_df),
            'filters_applied': filters
        })

    except Exception as e:
        app.logger.error(f'Error getting filtered WDA_Reg aggregations: {str(e)}')
        log_user_activity(username, 'WDA_REG_FILTERED_ERROR', f'Error getting filtered aggregations: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/download-wda-reg-filtered', methods=['POST'])
@login_required
@permission_required('view')
def download_wda_reg_filtered():
    """API endpoint to download filtered WDA_Reg data"""
    username = session['username']
    try:
        app.logger.info(f'User {username} downloading filtered WDA_Reg data')
        log_user_activity(username, 'WDA_REG_DOWNLOAD', 'Downloaded filtered WDA_Reg data')

        global wda_reg_system_data, wda_reg_data_loaded

        # Get data from memory cache
        if not wda_reg_data_loaded or wda_reg_system_data.empty:
            if not load_wda_reg_system_data():
                df = get_wda_reg_aggregated_data()
            else:
                df = wda_reg_system_data.copy()
        else:
            df = wda_reg_system_data.copy()

        # Apply filters from request
        filters = request.json or {}
        filtered_df = apply_wda_reg_filters(df, filters)

        # Create temporary file for download
        system_monitor_dir = os.path.join(os.getcwd(), "system_Monitor")
        os.makedirs(system_monitor_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_filename = f'wda_reg_filtered_data_{timestamp}.csv'
        temp_filepath = os.path.join(system_monitor_dir, temp_filename)

        # Save filtered data
        filtered_df.to_csv(temp_filepath, index=False)

        app.logger.info(f'Successfully created filtered WDA_Reg download file: {temp_filepath}')

        return send_file(temp_filepath, as_attachment=True, download_name=temp_filename)

    except Exception as e:
        app.logger.error(f'Error downloading filtered WDA_Reg data: {str(e)}')
        log_user_activity(username, 'WDA_REG_DOWNLOAD_ERROR', f'Error downloading WDA_Reg data: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/upload-to-monitor', methods=['POST'])
@login_required
@permission_required('view')
def upload_to_monitor():
    """API endpoint to upload files to the monitor"""
    username = session['username']
    try:
        app.logger.info(f'User {username} uploading file to monitor')
        log_user_activity(username, 'WDA_REG_UPLOAD', 'Uploaded file to monitor')

        # Get file details from request
        data = request.json
        file_name = data.get('fileName')
        filters = data.get('filters', {})


        if not file_name:
            return jsonify({'status': 'error', 'message': 'File name is required'}), 400
        if not filters:
            filters = {}
            
        app.logger.info(f'File name: {file_name}, Filters: {filters}')
        
        # Query database directly for raw data with filters
        df = query_wda_reg_raw_data_with_filters(filters)
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No data found for the given filters'}), 404
		
        date_str = datetime.now().date()
        new_filename = f"{file_name}@{date_str}.txt"
        file_path = os.path.join(Config.WORK_FOLDER, new_filename)
        file = df[['PART_NUMBER','MAN_NAME','MODULE_NAME']].copy()
        upload_file_to_amazon(file,new_filename)
        try:
            file.to_csv(file_path, index=False, sep='\t')
            app.logger.info(f'User {username} uploaded file: {new_filename}')
            log_user_activity(username, 'FILE_UPLOAD', f'Uploaded: {new_filename}')
            main_upload_parts(Config.WORK_FOLDER)
            app.logger.info('File processed successfully')
            return jsonify({'message': 'File uploaded and processed successfully'})
        except Exception as e:
            app.logger.error(f'Error processing file: {str(e)}')
            log_user_activity(username, 'FILE_UPLOAD_ERROR', f'Failed to upload: {file_name} - {str(e)}')
            return jsonify({'error': str(e)}), 500

        

    except Exception as e:
        app.logger.error(f'Error uploading file to monitor: {str(e)}')
        log_user_activity(username, 'WDA_REG_UPLOAD_ERROR', f'Error uploading file: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500



@app.route('/api/download-wda-reg-raw-data', methods=['POST'])
@login_required
@permission_required('view')
def download_wda_reg_raw_data():
    """API endpoint to download raw WDA_Reg data directly from database with filters"""
    username = session['username']
    try:
        app.logger.info(f'User {username} downloading raw WDA_Reg data from database')
        log_user_activity(username, 'WDA_REG_RAW_DOWNLOAD', 'Downloaded raw WDA_Reg data from database')

        # Get filters from request
        filters = request.json or {}

        # Query database directly for raw data with filters
        df = query_wda_reg_raw_data_with_filters(filters)

        # Create temporary file for download
        system_monitor_dir = os.path.join(os.getcwd(), "system_Monitor")
        os.makedirs(system_monitor_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_filename = f'wda_reg_raw_data_{timestamp}.csv'
        temp_filepath = os.path.join(system_monitor_dir, temp_filename)

        # Save raw data
        df.to_csv(temp_filepath, index=False)

        app.logger.info(f'Successfully created raw WDA_Reg download file: {temp_filepath} with {len(df)} records')

        return send_file(temp_filepath, as_attachment=True, download_name=temp_filename)

    except Exception as e:
        app.logger.error(f'Error downloading raw WDA_Reg data: {str(e)}')
        log_user_activity(username, 'WDA_REG_RAW_DOWNLOAD_ERROR', f'Error downloading raw data: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/download-summary-timeline-data', methods=['POST'])
@login_required
@permission_required('view')
def download_summary_timeline_data():
    """API endpoint to download summary timeline data in CSV format"""
    username = session['username']
    try:
        app.logger.info(f'User {username} downloading summary timeline data')
        log_user_activity(username, 'SUMMARY_TIMELINE_DOWNLOAD', 'Downloaded summary timeline data')

        # Get filters from request
        filters = request.json or {}

        # Get the aggregated data with filters
        df = get_wda_reg_aggregated_data()
        
        aggregations = calculate_wda_reg_aggregations(df)
        
        # Extract summary timeline data
        summary_timeline = aggregations['charts']['summary_timeline']
        
        # Create DataFrame from summary timeline data
        summary_df = pd.DataFrame({
            'Date': summary_timeline['dates'],
            'Count': summary_timeline['counts']
        })
        
        # Create temporary file for download
        system_monitor_dir = os.path.join(os.getcwd(), "system_Monitor")
        os.makedirs(system_monitor_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_filename = f'summary_timeline_data_{timestamp}.csv'
        temp_filepath = os.path.join(system_monitor_dir, temp_filename)

        # Save summary timeline data
        summary_df.to_csv(temp_filepath, index=False)

        app.logger.info(f'Successfully created summary timeline download file: {temp_filepath} with {len(summary_df)} records')

        return send_file(temp_filepath, as_attachment=True, download_name=temp_filename)

    except Exception as e:
        app.logger.error(f'Error downloading summary timeline data: {str(e)}')
        log_user_activity(username, 'SUMMARY_TIMELINE_DOWNLOAD_ERROR', f'Error downloading summary timeline data: {str(e)}')
        return jsonify({'status': 'error', 'message': str(e)}), 500


def query_wda_reg_raw_data_with_filters(filters):
    """Query database directly for raw WDA_Reg data with filters applied"""
    

    engine = create_db_engine()

    try:
        # Build the base query with filters applied in SQL
        base_query = """
            with 
                LC_outdated as(
                SELECT 
                    u.COM_PARTNUM AS PN, 
                    u.MAN_ID AS MAN_id
                FROM cm.xlp_se_component u 
                LEFT JOIN cm.tbl_lc_hstry bb ON u.COM_ID = bb.COM_ID 
                LEFT JOIN cm.tbl_lc_lookup_db c ON bb.lc_lookup_id = c.lc_lookup_id 
                LEFT JOIN cm.tbl_lc_src_reason d ON c.sorce_reason_id = d.sorce_reason_id  
                LEFT JOIN cm.tbl_lc_src_typ e ON d.lc_src_id = e.lc_src_id    
                WHERE
                    e.lc_src_name IN ('Supplier Site_Auto', 'Supplier Site_Auto_S', 'WDA', 'WDA_M', 'WDA_S', 'WDA_A', 'WDA_A_S')
                    AND bb.LATEST = 1
                    AND u.NAN_PARTNUM NOT LIKE '%)$.@(%'
                    AND (NOT (( CAST(cm.xlp_releasedate_function_d(bb.last_checked_date) AS DATE) >= ( SYSDATE - 90 ))
                    OR ( c.se_lc = 'Obsolete' )
                    OR ( c.se_lc = 'LTB' AND bb.se_lc_date IS NOT NULL )) 
                    OR bb.last_checked_date IS NULL)
                ),latest_status as (
                    SELECT mpn,man_id, status
                    FROM (
                        SELECT mpn, status,man_id, check_date,
                            ROW_NUMBER() OVER (PARTITION BY mpn ORDER BY check_date DESC) AS rn
                        FROM webspider.tbl_prsys_feed_notfound@new3_n
                    )
                    WHERE rn = 1
                )
                ,main_data as(
                    SELECT 
                        pn,
                        man_id,
                        mod_id,
                        Prty,
                        cs,
                        LRD2,
                        v_notfound_dat2,  
                        CASE 
                            WHEN v_notfound_dat2 > LRD2 THEN 'not found'
                            WHEN LRD2 > v_notfound_dat2 THEN 'found'
                            WHEN LRD2 = TO_DATE('01-JAN-1970', 'DD-MON-YYYY') 
                                AND v_notfound_dat2 = TO_DATE('01-JAN-1970', 'DD-MON-YYYY') THEN 'not run'
                        END AS status,

                        
                        CASE 
                            WHEN v_notfound_dat2 > LRD2 THEN v_notfound_dat2
                            when LRD2 > v_notfound_dat2 then LRD2
                            ELSE Null
                        END AS LR_date,
                        lc_outdated,
                        error_status

                    FROM (
                        SELECT 
                            a.pn,
                            a.man_id,
                            a.mod_id,
                            a.Prty,
                            a.cs,
                            NVL(TO_DATE(a.v_notfound_dat, 'DD-MON-YYYY'), TO_DATE('01-JAN-1970', 'DD-MON-YYYY')) AS v_notfound_dat2,
                            NVL(cm.XLP_RELEASEDATE_FUNCTION_D(a.LRD), TO_DATE('01-JAN-1970', 'DD-MON-YYYY')) AS LRD2,
                            case 
                                when b.pn is null then 0 else 1
                            end as lc_outdated,
                            ls.status as error_status
                        FROM updatesys.TBL_Prty_pns_@NEW3_N a left join LC_outdated b on a.man_id = b.man_id and a.pn = b.pn
                        left join latest_status ls on ls.mpn = a.pn and ls.man_id = a.man_id 
                        
                    )
                    )

                select
                    x.pn,
                    z.man_name,
                    y.module_name,
                    y.wda_flag,
                    x.Prty,
                    x.cs,
                    x.LRD2,
                    x.v_notfound_dat2, 
                    x.status,
                    x.LR_date,
                    x.lc_outdated,
                    x.error_status
                from main_data x join updatesys.tbl_man_modules@new3_n y on x.man_id = y.man_id and x.mod_id = y.module_id
                join cm.xlp_se_manufacturer@new3_n z on y.man_id = z.man_id 

        """

        # Build WHERE clause based on filters
        where_conditions = []
        query_params = {}

        if filters.get('man_names') and len(filters['man_names']) > 0:
            placeholders = ','.join([f':man_name_{i}' for i in range(len(filters['man_names']))])
            where_conditions.append(f"z.man_name IN ({placeholders})")
            for i, man_name in enumerate(filters['man_names']):
                query_params[f'man_name_{i}'] = man_name

        if filters.get('module_names') and len(filters['module_names']) > 0:
            placeholders = ','.join([f':module_name_{i}' for i in range(len(filters['module_names']))])
            where_conditions.append(f"y.module_name IN ({placeholders})")
            for i, module_name in enumerate(filters['module_names']):
                query_params[f'module_name_{i}'] = module_name

        if filters.get('priorities') and len(filters['priorities']) > 0:
            placeholders = ','.join([f':priority_{i}' for i in range(len(filters['priorities']))])
            where_conditions.append(f"x.Prty IN ({placeholders})")
            for i, priority in enumerate(filters['priorities']):
                query_params[f'priority_{i}'] = priority

        if filters.get('statuses') and len(filters['statuses']) > 0:
            placeholders = ','.join([f':status_{i}' for i in range(len(filters['statuses']))])
            where_conditions.append(f"x.status IN ({placeholders})")
            for i, status in enumerate(filters['statuses']):
                query_params[f'status_{i}'] = status

        if filters.get('wda_flags') and len(filters['wda_flags']) > 0:
            placeholders = ','.join([f':wda_flag_{i}' for i in range(len(filters['wda_flags']))])
            where_conditions.append(f"y.wda_flag IN ({placeholders})")
            for i, wda_flag in enumerate(filters['wda_flags']):
                query_params[f'wda_flag_{i}'] = wda_flag

        if filters.get('lc_outdated_filter') and len(filters['lc_outdated_filter']) > 0:
            placeholders = ','.join([f':lc_outdated_{i}' for i in range(len(filters['lc_outdated_filter']))])
            where_conditions.append(f"x.lc_outdated IN ({placeholders})")
            for i, lc_outdated in enumerate(filters['lc_outdated_filter']):
                query_params[f'lc_outdated_{i}'] = lc_outdated

        # Add date filters if provided
        if filters.get('date_start'):
            where_conditions.append("x.LR_date >= TO_DATE(:date_start, 'YYYY-MM-DD')")
            query_params['date_start'] = filters['date_start']

        if filters.get('date_end'):
            where_conditions.append("x.LR_date <= TO_DATE(:date_end, 'YYYY-MM-DD')")
            query_params['date_end'] = filters['date_end']
        if filters.get('error_status'):
            where_conditions.append("x.error_status IN (:error_status)")
            query_params['error_status'] = filters['error_status']
            
        # Add CS labels filter if provided
        if filters.get('cs_labels') and len(filters['cs_labels']) > 0:
            cs_conditions = []
            for i, cs_label in enumerate(filters['cs_labels']):
                cs_conditions.append(f"x.cs LIKE :cs_label_{i}")
                query_params[f'cs_label_{i}'] = f'%{cs_label}%'
            where_conditions.append(f"({' OR '.join(cs_conditions)})")

        # Combine base query with WHERE clause
        if where_conditions:
            final_query = base_query + " WHERE " + " AND ".join(where_conditions)
        else:
            final_query = base_query

        # Add ORDER BY for consistent results
        #final_query += " ORDER BY z.man_name, y.module_name, x.Prty, x.status"

        app.logger.info(f'Executing raw data query with {len(query_params)} filter parameters')

        with engine.connect() as connection:
            result = connection.execute(text(final_query), query_params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df.columns)
        df.columns = [c.upper() for c in df.columns]
        # Process dates and add additional computed columns
        df['LRD2'] = pd.to_datetime(df['LRD2'], errors='coerce')
        df['V_NOTFOUND_DAT2'] = pd.to_datetime(df['V_NOTFOUND_DAT2'], errors='coerce')
        df['LR_DATE'] = pd.to_datetime(df['LR_DATE'], errors='coerce')

        # Add is_expired flag based on LR_DATE
        df['is_expired'] = (df['LR_DATE'].isna()) | (df['LR_DATE'] < datetime.now() - timedelta(days=Config.Date_to_expire))

        # Apply expired filter if provided
        if filters.get('expired_filter') and len(filters['expired_filter']) > 0:
            expired_values = [val.lower() == 'true' for val in filters['expired_filter']]
            df = df[df['is_expired'].isin(expired_values)]

        app.logger.info(f'Raw data query completed, returning {len(df)} records')
        return df

    except Exception as e:
        app.logger.error(f'Error in query_wda_reg_raw_data_with_filters: {str(e)}')
        app.logger.error(traceback.format_exc())
        raise
    finally:
        engine.dispose()

def apply_wda_reg_filters(df, filters):
    """Apply filters to WDA_Reg data"""
    filtered_df = df
    start_time = time.time()

    if filters.get('man_names'):
        filtered_df = filtered_df[filtered_df['MAN_NAME'].isin(filters['man_names'])]
    print("time for filter man names: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('module_names'):
        filtered_df = filtered_df[filtered_df['MODULE_NAME'].isin(filters['module_names'])]
    print("time for filter module names: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('priorities'):
        filtered_df = filtered_df[filtered_df['PRTY'].isin(filters['priorities'])]
    print("time for filter priorities: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('statuses'):
        filtered_df = filtered_df[filtered_df['STATUS'].isin(filters['statuses'])]
    print("time for filter statuses: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('wda_flags'):
        filtered_df = filtered_df[filtered_df['WDA_FLAG'].isin(filters['wda_flags'])]
    
    print("time for filter wda flags: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('error_status'):
        filtered_df = filtered_df[filtered_df['ERROR_STATUS'].isin(filters['error_status'])]
    
    print("time for filter error status: ",time.time()-start_time)
    start_time = time.time()

    if filters.get('lc_outdated_filter'):
        filtered_df = filtered_df[filtered_df['LC_OUTDATED'].isin([int(x) for x in filters['lc_outdated_filter']])]
    print("time for filter lc outdated: ",time.time()-start_time)
    start_time = time.time()
    if filters.get('expired_filter'):
        if 'true' in filters['expired_filter'] and 'false' not in filters['expired_filter']:
            filtered_df = filtered_df[filtered_df['is_expired'] == True]
        elif 'false' in filters['expired_filter'] and 'true' not in filters['expired_filter']:
            filtered_df = filtered_df[filtered_df['is_expired'] == False]
    print("time for filter expired: ",time.time()-start_time)
    start_time = time.time()
    # CS filter (optimized multi-label support)
    if filters.get('cs_labels'):
        cs_filter_labels = filters['cs_labels']
        # Use vectorized string operations for better performance
        if 'CS_clean' not in filtered_df.columns:
            filtered_df['CS_clean'] = filtered_df['CS'].fillna('').astype(str).replace('nan', '')

        # Create regex pattern to match any of the selected labels
        pattern = '|'.join([f'\\b{re.escape(label)}\\b' for label in cs_filter_labels])
        mask = filtered_df['CS_clean'].str.contains(pattern, regex=True, na=False)
        filtered_df = filtered_df[mask]
    print("time for filter cs labels: ",time.time()-start_time)



    start_time = time.time()
    # Date range filter for timeline
    if filters.get('date_start') or filters.get('date_end'):
        if 'LR_DATE' in filtered_df.columns:
            filtered_df['LR_DATE'] = pd.to_datetime(filtered_df['LR_DATE'], errors='coerce')

            if filters.get('date_start'):
                start_date = pd.to_datetime(filters['date_start']).date()
                filtered_df = filtered_df[filtered_df['LR_DATE'].dt.date >= start_date]

            if filters.get('date_end'):
                end_date = pd.to_datetime(filters['date_end']).date()
                filtered_df = filtered_df[filtered_df['LR_DATE'].dt.date <= end_date]

    return filtered_df

@app.route('/import-status')
def import_status():
    return render_template('import_status.html')

@app.route('/run-import-status', methods=['POST'])
def run_import_status():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400

        # Run the automation process
        results = automate_process(last_done_date=start_date, last_date=end_date)
        print(results.head())
        # Process results to include required counts
        processed_results = []
        for date in results['date'].unique():
            date_data = results[results['date'] == date]

            # Process found parts
            found_data = date_data[date_data['table'] == 'found'].iloc[0] if len(date_data[date_data['table'] == 'found']) > 0 else None
            not_found_data = date_data[date_data['table'] == 'notfound'].iloc[0] if len(date_data[date_data['table'] == 'notfound']) > 0 else None

            if found_data is not None:
                processed_results.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'table': 'found',
                    'total_parts': int(found_data['total_parts']),
                    'in_progress_count': int(found_data['in_progress_count']),
                    'not_received_count': int(found_data['not_received_count']),
                    'received_count': int(found_data['received_count']),
                    'imported_count': int(found_data['imported_count']),
                    'not_imported_count': int(found_data['not_imported_count'])
                })

            if not_found_data is not None:
                processed_results.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'table': 'notfound',
                    'total_parts': int(not_found_data['total_parts']),
                    'in_progress_count': int(not_found_data['in_progress_count']),
                    'not_received_count': int(not_found_data['not_received_count']),
                    'received_count': int(not_found_data['received_count']),
                    'imported_count': int(not_found_data['imported_count']),
                    'not_imported_count': int(not_found_data['not_imported_count'])
                })

        return jsonify({
            'message': 'Import status check completed successfully',
            'results': processed_results
        })
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/download-reports', methods=['POST'])
def download_reports():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400

        # Download reports using the existing Download_autoImported function
        feed_hour, notApproved_hour = Download_autoImported(start_date, end_date)
        patterns = move_exported_files(Config.prty_feed_path, 'automate_records',
                                     feed_hour=feed_hour, notApproved_hour=notApproved_hour)

        # Get list of available dates from the moved files
        available_dates = []
        for pattern in patterns:
            date_match = re.search(r'\d{2}-[A-Za-z]{3}-\d{4}', pattern)
            if date_match:
                date_str = date_match.group()
                formatted_date = datetime.strptime(date_str, '%d-%b-%Y').strftime('%Y-%m-%d')
                available_dates.append(formatted_date)

        return jsonify({
            'message': 'Reports downloaded successfully',
            'dates': sorted(list(set(available_dates)))
        })
    except Exception as e:
        app.logger.error(f'Error downloading reports: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/download-part-details', methods=['POST'])
def download_part_details():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'error': 'Start date and end date are required'}), 400

        # Download part details using existing functions
        check_and_download_missing_dates(start_date, end_date, Config.daily_feed_url, Config.daily_feed_path)
        extract_part_details(Config.daily_feed_path, 'automate_records', start_date, end_date)

        return jsonify({'message': 'Part details downloaded successfully'})
    except Exception as e:
        app.logger.error(f'Error downloading part details: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/calculate-status', methods=['POST'])
def calculate_status():
    try:
        data = request.get_json()
        dates = data.get('dates')

        if not dates:
            return jsonify({'error': 'No dates selected'}), 400

        # Find the corresponding report files for the selected dates
        results = []
        for date in dates:
            #formatted_date = datetime.strptime(date, '%Y-%m-%d').strftime('%d-%b-%Y')
            found_file = f'Export_prysysFeed_{date}.txt'
            not_found_file = f'Export_NotFound_prysysFeed_{date}.txt'

            # Find matching files
            found_matches = glob.glob(os.path.join('automate_records', found_file))
            not_found_matches = glob.glob(os.path.join('automate_records', not_found_file))

            if found_matches and not_found_matches:
                result = calculate_import_status(found_matches[0], not_found_matches[0], 'automate_records')
                results.extend(result.to_dict('records'))
        print("calculate_import_status completed successfully")
        return jsonify({
            'message': 'Status calculated successfully',
            'results': results
        })
    except Exception as e:
        app.logger.error(f'Error calculating status: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/download-report/<report_type>/<start_date>/<end_date>')
def download_report(report_type, start_date, end_date):
    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

        # Initialize empty list to store dataframes
        all_dfs = []
        current_date = start_date_obj

        # Loop through each date in the range
        while current_date <= end_date_obj:
            formatted_date = current_date.strftime('%Y-%m-%d')

            if report_type == 'imported':
                filename = f'Imported_NotImported_parts_{formatted_date}.csv'
            else:  # missed
                filename = f'Missed_Inprogress_parts_{formatted_date}.csv'

            file_path = os.path.join('automate_records', filename)

            if os.path.exists(file_path):
                try:
                    # Read the CSV file and add date column
                    df = pd.read_csv(file_path,low_memory=False)
                    df['report_date'] = formatted_date
                    all_dfs.append(df)
                except Exception as e:
                    app.logger.error(f'Error reading file {filename}: {str(e)}')

            current_date += timedelta(days=1)

        if not all_dfs:
            return jsonify({'error': 'No report files found for the specified date range'}), 404

        # Concatenate all dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Create a temporary file for the combined results
        temp_dir = os.path.join('automate_records', 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        if report_type == 'imported':
            output_filename = f'Combined_Imported_NotImported_parts_{start_date}_to_{end_date}.csv'
        else:
            output_filename = f'Combined_Missed_Inprogress_parts_{start_date}_to_{end_date}.csv'

        temp_file_path = os.path.join(temp_dir, output_filename)

        # Save the combined DataFrame to CSV
        combined_df.to_csv(temp_file_path, index=False)

        # Send the combined file
        return send_file(
            temp_file_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=output_filename
        )

    except Exception as e:
        app.logger.error(f'Error in download_report: {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        # Cleanup: Remove temporary file after sending
        try:
            if 'temp_file_path' in locals():
                os.remove(temp_file_path)
        except Exception as e:
            app.logger.error(f'Error cleaning up temporary file: {str(e)}')

@app.route('/download-not-approved')
def download_not_approved():
    try:
        seven_zip_path = os.path.join('automate_records', 'Latest_Not_Approved.7z')

        if not os.path.exists(seven_zip_path):
            return jsonify({
                'error': 'No not approved file is currently available. Please try again later.'
            }), 404

        # Get the file's last modification time
        mod_time = datetime.fromtimestamp(os.path.getmtime(seven_zip_path))
        current_time = datetime.now()

        # Check if file is older than 24 hours
        if (current_time - mod_time).days >= 1:
            return jsonify({
                'error': 'The available file is more than 24 hours old. Please generate a new export first.'
            }), 404

        return send_file(
            seven_zip_path,
            mimetype='application/x-7z-compressed',
            as_attachment=True,
            download_name=f'Not_Approved_Parts_{mod_time.strftime("%Y%m%d_%H%M")}.7z'
        )

    except Exception as e:
        app.logger.error(f'Error in download_not_approved: {str(e)}')
        return jsonify({'error': str(e)}), 500

def daily_task():
	daily_check_all()

# Config for APScheduler
def download_matrix_task():
	global matrix_df
	download_matrix_toFile()
	matrix_df = pd.read_csv(r'Static Data\matrix.csv')

def weekly_scheduled_upload_task():
	"""
	Weekly task to upload expired parts from scheduled files to Amazon
	"""
	global amazon_upload_in_progress, scheduled_files

	app.logger.info(f'Running weekly scheduled upload for {len(scheduled_files)} files')

	if not scheduled_files:
		app.logger.info('No files scheduled for upload')
		return

	if amazon_upload_in_progress:
		app.logger.warning('Another upload to Amazon is in progress. Skipping scheduled upload.')
		return

	try:
		with amazon_upload_lock:
			amazon_upload_in_progress = True

			for filename in scheduled_files:
				try:
					app.logger.info(f'Processing scheduled upload for {filename}')
					file_path = os.path.join(Config.WORK_FOLDER, filename)

					if not os.path.exists(file_path):
						app.logger.warning(f'Scheduled file not found: {filename}')
						continue

					# Get only expired parts
					df = pd.read_csv(file_path, sep='\t',low_memory=False)
					today = datetime.now().date()

					# Filter for expired parts
					df_filtered = df[df['stop_monitor_date'].notna()]
					df_filtered['stop_monitor_date'] = pd.to_datetime(df_filtered['stop_monitor_date']).dt.date
					df_expired = df_filtered[df_filtered['stop_monitor_date'] < today]

					if df_expired.empty:
						app.logger.info(f'No expired parts found in {filename}')
						continue

					# Upload to Amazon
					timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
					upload_filename = f'{filename}_expired_{timestamp}'
					upload_file_to_amazon(df_expired, upload_filename)
					app.logger.info(f'Successfully uploaded expired parts from {filename} to Amazon')

				except Exception as e:
					app.logger.error(f'Error processing scheduled upload for {filename}: {str(e)}')

	except Exception as e:
		app.logger.error(f'Error in weekly scheduled upload task: {str(e)}')
	finally:
		amazon_upload_in_progress = False







def wda_reg_system_download_task():
	"""
	Daily task to download WDA_Reg system aggregated data and reload into memory
	"""
	try:
		app.logger.info('Running daily WDA_Reg system data download task')
		download_wda_reg_system_data()

		# Reload data into memory cache
		load_wda_reg_system_data()

		app.logger.info('WDA_Reg system data download task completed successfully and reloaded into memory')
	except Exception as e:
		app.logger.error(f'Error in WDA_Reg system data download task: {str(e)}')
# 🔹 Define TaskConfig


def daily_summary_calculation():
      try:
            app.logger.info('Running daily summary calculation task')
            run_daily_summary()
            app.logger.info('Daily summary calculation task completed successfully')
      except Exception as e:
            app.logger.error(f'Error in daily summary calculation task: {str(e)}')



def get_summary():
    path = os.path.join(Config.result_path,'summary.csv')
    if os.path.exists(path) and datetime.fromtimestamp(os.path.getmtime(path)).date() == datetime.now().date():
        df = pd.read_csv(path,low_memory=False)
        df = df.sort_values(by='summary_date')
        print("returning from cache")
        return df
    else:
        print("downloading from database")
        return download_summary_from_database()


class TaskConfig:
    SCHEDULER_API_ENABLED = True

    SCHEDULER_EXECUTORS = {
        'default': {'type': 'threadpool', 'max_workers': 5}
    }
    SCHEDULER_JOB_DEFAULTS = {
        'coalesce': True,  # Combine missed runs into one
        'max_instances': 2,  # Allow at most 2 concurrent instances
        'misfire_grace_time': 3600  # Allow 1-hour delay execution
    }
    JOBS = [
        {
            'id': 'daily_task',  # Unique Job ID
            'func': 'app:daily_task',  # Function to run
            'trigger': 'cron',
            'hour': 1,
            'minute': 0
        },
		{
            'id': 'download_matrix_task',  # Unique Job ID
            'func': 'app:download_matrix_task',  # Function to run
            'trigger': 'cron',
            'hour': 7,
            'minute': 0
        },
        {
            'id': 'wda_reg_system_download',  # Unique Job ID
            'func': 'app:wda_reg_system_download_task',  # Function to run
            'trigger': 'cron',
            'hour': 7,  # At 7 AM daily
            'minute': 26
        },
        {
            'id': 'weekly_scheduled_upload',  # Unique Job ID
            'func': 'app:weekly_scheduled_upload_task',  # Function to run
            'trigger': 'cron',
            'day_of_week': 'fri',  # Run every Monday
            'hour': 3,  # At 3 AM
            'minute': 0
        },
        {
            'id': 'daily_summary',  # Unique Job ID
            'func': 'app:daily_summary_calculation',  # Function to run
            'trigger': 'cron',
            'hour': 10,  # At 5 AM daily
            'minute': 36
        }
    ]





@app.route('/get-available-dates')
def get_available_dates():
    try:
        # Get list of available dates from your storage location
        available_dates = []
        reports_dir = 'automate_records'  # Adjust this to your reports directory

        # Get all report files and extract dates
        for file in os.listdir(reports_dir):
            if file.endswith('.txt') and 'NotFound_prysysFeed' in file:
                pattern = r"(\d{2}-[A-Za-z]{3}-\d{4})_(\d{2}_[APM]{2})"
                # Search for the pattern in the filename
                date_match = re.search(pattern, file)
                if date_match:
                    date_str = date_match.group()
                    available_dates.append(date_str)

        # Remove duplicates and sort
        available_dates = sorted(list(set(available_dates)), reverse=True)

        return jsonify({
            'dates': available_dates
        })
    except Exception as e:
        app.logger.error(f'Error getting available dates: {str(e)}')
        return jsonify({'error': str(e)}), 500


# Add config to Flask app
app.config.from_object(TaskConfig)

# Initialize APScheduler
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


@app.route('/api/upload-filtered-to-amazon', methods=['POST'])
def upload_filtered_to_amazon():
    global amazon_upload_in_progress, grouped_data

    if amazon_upload_in_progress:
        return jsonify({'error': 'Another upload to Amazon is in progress. Please wait.'}), 429

    try:
        with amazon_upload_lock:
            amazon_upload_in_progress = True

            # Get filters from request
            filters = request.json
            df = global_df.copy()
            df = Get_filtered_data(df , filters)
            # Apply filters

            # Generate a unique filename for the filtered data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'filtered_data_{timestamp}.csv'

            # Upload the filtered data to Amazon
            upload_file_to_amazon(df, filename)

            return jsonify({'message': 'Filtered data uploaded to Amazon successfully'})
    except Exception as e:
        app.logger.error(f'Error uploading filtered data to Amazon: {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        amazon_upload_in_progress = False



if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5022, threaded=True,debug=True)





