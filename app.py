import os
import logging
import pandas as pd
from logging.handlers import RotatingFileHandler
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
from sqlalchemy import text
from werkzeug.utils import secure_filename
from datetime import datetime
from Parts_Upload import main_upload_parts, main_delete_file
from check_status import Get_status, Download_results, get_status_statistics
from config import Config
import threading
import filelock
import tempfile
import traceback
import time

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
print("Done importing...")
# Add Oracle Client to PATH
os.environ["PATH"] = os.path.join(os.path.dirname(__file__), Config.INSTANT_CLIENT) + ";" + os.environ["PATH"]
# Global DataFrames
global_df = None
grouped_data = None


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
		global_df['status'] = global_df['status'].apply(lambda x: 
			'Proxy' if '403' in str(x) else
			'Error' if any(err in str(x) for err in ['Error', 'Exception', 'Incomplete']) else
			x
		)
		
		global_df['status'].fillna('-',inplace=True)
		global_df['prty'].fillna('-',inplace=True)
		global_df['table_name'].fillna('-',inplace=True)
		
		grouped_data = global_df.groupby(
    ['man', 'module', 'file_id', 'status', 'last_run_date', 
     'table_name', 'prty', 'file_name', 'is_expired',], dropna=False
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
	
	return render_template('index.html', files=files, db_files=db_files, today=today)

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
			'priorities': sorted(grouped_data['prty'].unique().tolist()),
			'table_name': sorted(grouped_data[grouped_data['table_name'].notna()]['table_name'].unique().tolist())
		}
		filter_options = jsonify(filers_dict)
		print(filter_options)
		return filter_options

	except Exception as e:
		app.logger.error(f'Error fetching filter options: {str(e)}')
		
		return jsonify({'error': str(e)}), 500

@app.route('/api/chart-data', methods=['GET','POST'])
def get_chart_data():
	try:
		app.logger.info('Fetching chart data')
		if grouped_data is None:
			load_data()
			
		df = grouped_data.copy()
		
		# Apply filters
		filters = request.json
		
		if filters.get('module'):
			df = df[df['module'].isin(filters['module'])]
		if filters.get('file_name'):
			df = df[df['file_name'].isin(filters['file_name'])]
		if filters.get('man'):
			df = df[df['man'].isin(filters['man'])]
		if filters.get('status'):
			stfilter =  filters['status']
			df = df[df['status'].isin(stfilter) ]
		if filters.get('prty'):
			ptfilters = filters['prty']
			df = df[df['prty'].isin(ptfilters) ]
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

		for module in df['module'].unique():
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
			
			#.map({0:'Stopped', 1:'Regular Running',2:'Run By Request',3:'schedule Running'})
			# Calculate last 3 days statistics
			recent_df = module_df[module_df['last_run_date'] >= three_days_ago]
			recent_count = recent_df['count'].sum()
			recent_percentage = round((recent_count / total_count) * 100, 2) if total_count > 0 else 0
			
			module_stats.append({
				'module': module,
				'Running_Status': running_status,
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
		file_stats = []
		for file_name in df['file_name'].unique():
			file_df = df[df['file_name'] == file_name]
			total_count = file_df['count'].sum()
			error_count = file_df[file_df['status'].isin(['Error', 'Proxy'])]['count'].sum()
			found_count = file_df[file_df['status'] == 'found']['count'].sum()
			Done_parts = df[file_df['last_run_date']>=file_df['upload_date']]['count'].sum()
			file_stats.append({
				'file': file_name,
				'total_count': int(total_count),
				'error_count': int(error_count),
				'error_percentage': round((error_count / total_count) * 100, 2) if total_count > 0 else 0,
				'found_count': int(found_count),
				'found_percentage': round((found_count / total_count) * 100, 2) if total_count > 0 else 0,
				'done_percentage': round((Done_parts / total_count) * 100, 2) if total_count > 0 else 0
			})

		# Sort by total count descending
		file_stats.sort(key=lambda x: x['total_count'], reverse=True)
		

		return_data = jsonify({
			'stats': stats,
			'tableData': module_stats,
    		'fileStats': file_stats,  # Add this line
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
		
		return return_data
	except Exception as e:
		app.logger.error(f'Error fetching chart data: {str(e)}')
		print(traceback.format_exc())
		return jsonify({'error': str(e)}), 500

@app.route('/api/download-filtered', methods=['POST'])
def download_filtered():
	try:
		app.logger.info('Downloading filtered results')
		filters = request.json
		df = pd.read_csv(os.path.join(Config.result_path, 'results.csv'))
		
		# Apply filters
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

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, threaded=True)



