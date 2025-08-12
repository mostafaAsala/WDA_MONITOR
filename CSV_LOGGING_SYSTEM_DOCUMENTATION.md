# CSV Logging System Documentation

## Overview
The WDA monitoring system now uses a comprehensive CSV-based logging system that creates separate log files for each scheduled task and process. Each log entry includes timestamp, parent process, subprocess, level, message, and details columns for easy monitoring and analysis.

## 🔧 **System Architecture**

### **Core Components**

1. **CSV Logger Setup** (`check_status.py`)
   - `setup_csv_logger(process_name, log_level=logging.INFO)` - Creates individual CSV loggers
   - Thread-safe CSV writing with global lock
   - Automatic CSV file creation with headers
   - Daily log file rotation (YYYY-MM-DD format)

2. **Process Lifecycle Tracking**
   - `log_process_start(logger, process_name, **kwargs)` - Log process initiation
   - `log_process_end(logger, process_name, success=True, **kwargs)` - Log process completion
   - `log_step(logger, step_description, subprocess_name=None, details=None)` - Log individual steps
   - `log_error_with_context(logger, exception, context_description, subprocess_name=None)` - Log errors with full context

### **CSV Log File Structure**
Each CSV log file contains the following columns:
- **timestamp**: YYYY-MM-DD HH:MM:SS format
- **parent_process**: Main process name (e.g., 'daily_task', 'matrix_download')
- **subprocess**: Specific subprocess or function name
- **level**: Log level (INFO, WARNING, ERROR)
- **message**: Log message content
- **details**: Additional context and parameters

## 📁 **Log File Organization**

### **Directory Structure**
```
logs/
├── daily_task.csv
├── matrix_download.csv
├── wda_system_download.csv
├── weekly_upload.csv
├── daily_summary.csv
├── file_upload.csv
├── status_check.csv
├── amazon_upload.csv
├── automation_process.csv
└── database_operations.csv
```

### **Process-Specific Loggers**
- **daily_task** - Scheduled daily status checking
- **matrix_download** - Matrix data download from SharePoint
- **wda_system_download** - WDA system data downloads
- **weekly_upload** - Weekly Amazon upload processes
- **daily_summary** - Daily summary calculations
- **file_upload** - File upload operations
- **file_delete** - File deletion operations
- **status_check** - Status checking operations
- **amazon_upload** - Amazon upload automation
- **automation_process** - General automation processes
- **database_operations** - Database query operations

## 🚀 **Implementation Details**

### **Updated Files**

#### **check_status.py**
- Replaced basic logging with CSV-based system
- Added thread-safe CSV handler class
- Implemented process lifecycle tracking functions
- Created individual loggers for each process type

#### **app.py**
- Updated all scheduled task functions to use subprocess names
- Enhanced error logging with context
- Added detailed step logging for each process phase

#### **Parts_Upload.py**
- Integrated CSV logging system with fallback support
- Added subprocess names for file processing steps
- Enhanced error handling with context logging

### **Key Features**

1. **Separate Log Files**: Each process gets its own CSV file for isolated monitoring
2. **Subprocess Tracking**: Detailed tracking of individual functions within processes
3. **Thread Safety**: Global lock ensures safe concurrent CSV writing
4. **Continuous Logging**: All log entries are appended to the same CSV files
5. **Structured Data**: CSV format allows easy analysis with Excel, pandas, or database tools
6. **Backward Compatibility**: Fallback to basic logging if CSV system unavailable

## 📊 **Usage Examples**

### **Basic Process Logging**
```python
logger = process_loggers['daily_task']
log_process_start(logger, "daily_status_check")
log_step(logger, "Query database for status", subprocess_name="database_query")
log_process_end(logger, "daily_status_check", success=True, records_processed=150)
```

### **Custom Record with Details**
```python
record = logger.makeRecord(
    logger.name, logging.INFO, '', 0, 
    f"Processing file {file_index}/{total_files}: {filename}", 
    (), None, func='file_processing'
)
record.subprocess = 'file_processing'
record.details = f'file_index={file_index},total_files={total_files},status=processing'
logger.handle(record)
```

### **Error Logging with Context**
```python
try:
    # Some operation
    pass
except Exception as e:
    log_error_with_context(logger, e, "File upload operation", subprocess_name="file_upload")
```

## 🔍 **Monitoring Benefits**

1. **Process Isolation**: Each scheduled task has its own log file for focused monitoring
2. **Subprocess Visibility**: Track individual functions and operations within processes
3. **Data Analysis**: CSV format enables easy analysis with spreadsheet tools or Python pandas
4. **Performance Tracking**: Timestamp data allows performance analysis
5. **Error Correlation**: Link errors to specific subprocesses and contexts
6. **Audit Trail**: Complete record of all process activities

## 🧪 **Testing**

Run the test script to verify the CSV logging system:
```bash
python test_csv_logging.py
```

This will create a test CSV log file and verify all logging functions work correctly.

## 📈 **Next Steps**

1. **Log Analysis Dashboard**: Create web interface to view and analyze CSV logs
2. **Log Retention Policy**: Implement automatic cleanup of old log files
3. **Real-time Monitoring**: Add live log streaming capabilities
4. **Alert System**: Set up notifications for error patterns
5. **Performance Metrics**: Add execution time tracking to all processes

## 📋 **Example CSV Output**

**File: logs/daily_task.csv**
```csv
timestamp,parent_process,subprocess,level,message,details
2024-07-30 09:00:01,daily_task,process_lifecycle,INFO,PROCESS START: daily_task_scheduler,
2024-07-30 09:00:02,daily_task,daily_check_execution,INFO,STEP: Execute daily check all process,
2024-07-30 09:00:15,daily_task,process_lifecycle,INFO,PROCESS END: daily_task_scheduler - SUCCESS,
2024-07-30 14:30:01,daily_task,process_lifecycle,INFO,PROCESS START: daily_task_scheduler,
2024-07-30 14:30:02,daily_task,daily_check_execution,INFO,STEP: Execute daily check all process,
2024-07-30 14:30:18,daily_task,process_lifecycle,INFO,PROCESS END: daily_task_scheduler - SUCCESS,
```

All log entries are continuously appended to the same CSV files, with the timestamp column providing chronological tracking.
