@echo off
title WDA_MONITOR
SET FLASK_ENV=production
SET FLASK_APP=wsgi.py

:: Activate virtual environment
call myenv\Scripts\activate.bat

:: Run with Waitress
python wsgi.py

:: Deactivate virtual environment
deactivate
pause