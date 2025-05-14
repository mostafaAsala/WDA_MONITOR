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
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
print("from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for")
from flask_apscheduler import APScheduler
print("from flask_apscheduler import APScheduler")
from sqlalchemy import text
print("from sqlalchemy import text")
from werkzeug.utils import secure_filename
print("from werkzeug.utils import secure_filename")
from datetime import datetime, timedelta
print("from datetime import datetime")
from Parts_Upload import main_upload_parts, main_delete_file
print("from Parts_Upload import main_upload_parts, main_delete_file")
from check_status import Get_status, Download_results, get_status_statistics, daily_check_all
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

def load_module_data():
	#load data from matrix man- module- running status - comment - old
	#load direct Feed suppliers
	#load man module table
	pass

def load_data():
	global global_df
	global grouped_data
	try:
		global_df = pd.read_csv(os.path.join(Config.result_path, 'results.csv'))
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
		return True
	except Exception as e:
		print(f"Error loading data: {e}")
		return False

def create_app():
	app = Flask(__name__)
	app.config.from_object(Config)

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

@app.route('/get-file-status', methods=['GET'])
def get_file_status():
	return jsonify(file_status)

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
	return jsonify({'status': 'success'})

@app.route('/')
def index():
	app.logger.info('Accessing index page')
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

	return render_template('index.html', files=files, db_files=db_files, today=today, scheduled_files=scheduled_files)

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
def visualizations():
	app.logger.info('Accessing visualizations page')
	return render_template('visuals.html')

@app.route('/upload', methods=['POST'])
def upload_file():
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
			app.logger.info(f'File saved: {new_filename}')
			main_upload_parts(Config.WORK_FOLDER)
			app.logger.info('File processed successfully')
			return jsonify({'message': 'File uploaded and processed successfully'})
		except Exception as e:
			app.logger.error(f'Error processing file: {str(e)}')
			return jsonify({'error': str(e)}), 500

@app.route('/delete/<filename>', methods=['POST'])
def delete_file(filename):
	try:
		app.logger.info(f'Attempting to delete file: {filename}')
		main_delete_file(Config.WORK_FOLDER, filename)
		app.logger.info(f'File deleted successfully: {filename}')
		return jsonify({'message': 'File deleted successfully'})
	except Exception as e:
		app.logger.error(f'Error deleting file {filename}: {str(e)}')
		return jsonify({'error': str(e)}), 500


@app.route('/upload-to-amazon', methods=['POST'])
def upload_to_amazon():
    global amazon_upload_in_progress
    filename = request.json.get('file_name')
    if not filename:
        return jsonify({'error': 'Filename is required'}), 400

    # Check if another upload is in progress
    if amazon_upload_in_progress:
        return jsonify({'error': 'Another upload to Amazon is in progress. Please wait.'}), 429

    try:
        with amazon_upload_lock:
            amazon_upload_in_progress = True
            file_path = os.path.join(Config.WORK_FOLDER, filename)
            df = pd.read_csv(file_path, sep='\t')
            upload_file_to_amazon(df,filename)
            return jsonify({'message': 'File uploaded to Amazon successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        amazon_upload_in_progress = False


@app.route('/status', methods=['POST'])
def check_status():
	selected_files = request.json.get('files', [])
	ignore_date = request.json.get('ignore_date', True)

	try:
		app.logger.info(f'Checking status for files: {selected_files}')

		# Update status to In Progress
		for file in selected_files:
			file_status[file] = 'In Progress'

		with status_lock:  # Ensure only one status check at a time
			files_string, daily_export = Get_status(selected_files, ignore_date)
			df, file_name  = Download_results(files_string, daily_export)
			# Use file lock when writing results
			with file_lock:
				status_stats = get_status_statistics(df)

			# Reset status to Idle
			for file in selected_files:
				file_status[file] = 'Idle'

			app.logger.info('Status check completed successfully')
			return_object = jsonify({
				'status': 'Success',
				'message': 'Status updated successfully',
				'data': status_stats
			})

			return return_object
	except Exception as e:
		# Reset status on error
		for file in selected_files:
			file_status[file] = 'Idle'
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

		from check_status import create_db_engine
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
                    df = pd.read_csv(file_path)
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
					df = pd.read_csv(file_path, sep='\t')
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
# 🔹 Define TaskConfig
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
            'id': 'weekly_scheduled_upload',  # Unique Job ID
            'func': 'app:weekly_scheduled_upload_task',  # Function to run
            'trigger': 'cron',
            'day_of_week': 'fri',  # Run every Monday
            'hour': 3,  # At 3 AM
            'minute': 0
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
	app.run(host='0.0.0.0', port=5000, threaded=True,debug=True)





