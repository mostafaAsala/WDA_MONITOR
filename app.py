import os
import logging
import pandas as pd
from logging.handlers import RotatingFileHandler
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
from werkzeug.utils import secure_filename
from datetime import datetime
from Parts_Upload import main_upload_parts, main_delete_file
from check_status import Get_status, Download_results, get_status_statistics
from config import Config


import traceback
print("Done importing...")
# Add Oracle Client to PATH
os.environ["PATH"] = os.path.join(os.path.dirname(__file__), Config.INSTANT_CLIENT) + ";" + os.environ["PATH"]
# Global DataFrame
global_df = None

def load_data():
    global global_df
    try:
        global_df = pd.read_csv(os.path.join(Config.result_path, 'results.csv'))
        global_df['status'] = global_df['status'].apply(lambda x: 
			'Proxy' if '403' in str(x) else
			'Error' if any(err in str(x) for err in ['Error', 'Exception', 'Incomplete']) else
			x
		)
        return True
    except Exception as e:
        print(f"Error loading data: {e}")
        return False

def create_app():
	app = Flask(__name__)
	app.config.from_object(Config)

	# Configure logging
	if not app.debug:
		if not os.path.exists('logs'):
			os.mkdir('logs')
		file_handler = RotatingFileHandler('logs/wda_monitor.log', maxBytes=10240, backupCount=10)
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

@app.route('/')
def index():
	app.logger.info('Accessing index page')
	files = os.listdir(Config.WORK_FOLDER)
	return render_template('index.html', files=files)

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
		df, file_name = Get_status(selected_files, ignore_date)
		status_stats = get_status_statistics(df)
		app.logger.info('Status check completed successfully')
		return_object = jsonify({
			'status': 'Success', 
			'message': 'Status updated successfully',
			'data': status_stats
		})
		
		return return_object
	except Exception as e:
		app.logger.error(f'Error checking status: {str(e)}')
		return jsonify({'error': str(e)}), 500

@app.route('/refresh-files', methods=['GET'])
def refresh_files():
	try:
		app.logger.info('Refreshing files list')
		files = os.listdir(Config.WORK_FOLDER)
		return jsonify({
			'status': 'Success',
			'files': files
		})
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
        if global_df is None:
            load_data()
        print(global_df['status'].dropna().unique().tolist())
        return jsonify({
            'teams': sorted(global_df['module'].unique().tolist()),
            'status': sorted(global_df['status'].dropna().unique().tolist()),
            'Files': sorted(global_df['file_name'].unique().tolist()),
            'projects': sorted(global_df['file_name'].unique().tolist()),
            'manufacturers': sorted(global_df[global_df['man'].notna()]['man'].unique().tolist()),
            'priorities': sorted(global_df[global_df['prty'].notna()]['prty'].unique().tolist()),
            'table_name': sorted(global_df[global_df['table_name'].notna()]['table_name'].unique().tolist())
        })
    except Exception as e:
        app.logger.error(f'Error fetching filter options: {str(e)}')
        app.logger.error(f'Error fetching filter options: {traceback.format_exc()}')
		
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart-data', methods=['POST'])
def get_chart_data():
	try:
		app.logger.info('Fetching chart data')
		if global_df is None:
			load_data()
            
		df = global_df.copy()
        
		# Modify status categorization
		

		# Apply filters
		filters = request.json
		if filters.get('module'):
			df = df[df['module'].isin(filters['module'])]
		if filters.get('file_name'):
			df = df[df['file_name'].isin(filters['file_name'])]
		if filters.get('man'):
			df = df[df['man'].isin(filters['man'])]
		if filters.get('status'):
			df = df[df['status'].isin(filters['status'])]
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
		
		
		
		# Calculate statistics
		stats = {
			'totalParts': len(df),
			'expiredParts': len(df[df['is_expired'] == True]),
			'activeParts': len(df[df['is_expired'] == False]),
			'errorParts': len(df[df['status'].isin(['Error', 'Proxy'])]),
			'missingParts': len(df[df['table_name'] == 'not run']),
			'moduleCount': len(df['module'].unique())
		}
		
		# Prepare chart data with modified status
		status_counts = df['status'].value_counts()
		expired_counts = df['is_expired'].value_counts()
		module_counts = df['module'].value_counts()
		table_name_counts = df['table_name'].value_counts()
		
		# Add timeline data
		df['last_run_date'] = pd.to_datetime(df['last_run_date'])
		timeline_data = df.groupby('last_run_date').size().reset_index()
		timeline_data = timeline_data.sort_values('last_run_date')
		# Calculate module statistics
		module_stats = []
		current_date = pd.Timestamp.now()
		three_days_ago = current_date - pd.Timedelta(days=3)

		for module in df['module'].unique():
			module_df = df[df['module'] == module]
			total_count = len(module_df)
			error_count = len(module_df[module_df['status'].isin(['Error', 'Proxy'])])
			found_count = len(module_df[module_df['status'] == 'found'])
			expired_count = len(module_df[module_df['is_expired'] == True])
			
			# Calculate last 3 days statistics
			recent_df = module_df[module_df['last_run_date'] >= three_days_ago]
			recent_count = len(recent_df)
			recent_percentage = round((recent_count / total_count) * 100, 2) if total_count > 0 else 0
			
			module_stats.append({
				'module': module,
				'total_count': total_count,
				'error_count': error_count,
				'error_percentage': round((error_count / total_count) * 100, 2) if total_count > 0 else 0,
				'found_count': found_count,
				'found_percentage': round((found_count / total_count) * 100, 2) if total_count > 0 else 0,
				'expired_count': expired_count,
				'expired_percentage': round((expired_count / total_count) * 100, 2) if total_count > 0 else 0,
				'recent_count': recent_count,
				'recent_percentage': recent_percentage
			})

		# Sort by total count descending
		module_stats.sort(key=lambda x: x['total_count'], reverse=True)

				

		
		sentdata =  jsonify({
			'stats': stats,
			'tableData': module_stats,
			'status': {
				'labels': status_counts.index.tolist(),
				'values': status_counts.values.tolist()
			},
			'isExpired': {
				'labels': ['Expired', 'Not Expired'],
				'values': [stats['expiredParts'], stats['activeParts']]
			},
			'team': {
				'labels': module_counts.index.tolist(),
				'values': module_counts.values.tolist()
			},
			'table_name': {
				'labels': table_name_counts.index.tolist(),
				'values': table_name_counts.values.tolist()
			},
			'timeline': {
				'dates': timeline_data['last_run_date'].dt.strftime('%Y-%m-%d').tolist(),
				'counts': timeline_data[0].tolist()
			}
		})
		return sentdata
	except Exception as e:
		app.logger.error(f'Error fetching chart data: {str(e)}')
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

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, threaded=True)