from waitress import serve
from app import app  # Replace `app` with your Flask app instance

if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=8000, threads=8)

exit(0)

"""import os
from app import create_app

# Set environment to production
os.environ['FLASK_ENV'] = 'production'

# Create the application instance
application = create_app()

if __name__ == "__main__":
	application.run()"""