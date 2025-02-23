import win32com.client

try:
    # Create and initialize Redemption RDOSession
    session = win32com.client.Dispatch("Redemption.RDOSession")
    session.RDOAcceptMessages = True
    print("Redemption initialized silently.")
except Exception as e:
    print(f"Error during silent initialization: {e}")
