import os
import tkinter as tk
from tkinter import filedialog, messagebox, Listbox
from datetime import datetime

import pandas as pd
from config import Config
# Import your functions
from Parts_Upload import main_upload_parts, main_delete_file
from check_status import Get_status, Download_results
# Define the working folder


# Function to browse and copy a file to the work folder
def browse_file():
    file_path = filedialog.askopenfilename(title="Select a File",filetypes=[("Text Files", "*.txt")])
    if file_path:
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(Config.WORK_FOLDER, file_name.split('.')[0]+'@'+str(datetime.now().date())+'.txt')
        try:
            # Copy the file to the work folder
            with open(file_path, "rb") as src, open(destination_path, "wb") as dest:
                dest.write(src.read())
            update_file_list()
            messagebox.showinfo("Success", f"File '{file_name}' loaded to the work folder.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load file: {e}")

# Function to upload files using main_upload_parts
def upload_files():
    try:
        main_upload_parts(Config.WORK_FOLDER)
        messagebox.showinfo("Success", "Files uploaded successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to upload files: {e}")

# Function to delete the selected file
def delete_file():

    selected_indices = file_listbox.curselection()
    # Retrieve the corresponding items
    selected_files = [file_listbox.get(i) for i in selected_indices]
    
    if not selected_files:
        messagebox.showwarning("Warning", "No files selected.")
        return
    
    for selected_file in selected_files:
        #selected_file = file_listbox.get(tk.ACTIVE)
        if not selected_file:
            messagebox.showwarning("Warning", "No file selected.")
            return
        try:
            main_delete_file(Config.WORK_FOLDER, selected_file)
            update_file_list()
            #messagebox.showinfo("Success", f"File '{selected_file}' deleted.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete file: {e}")
    messagebox.showinfo("Success", f"Files '{selected_files}' deleted.")

# Function to update the file listbox
def update_file_list():
    file_listbox.delete(0, tk.END)
    if os.path.exists(Config.WORK_FOLDER):
        files = os.listdir(Config.WORK_FOLDER)
        for file in files:
            file_listbox.insert(tk.END, file)

# Function to get the status
def get_status():
    try:
        selected_indices = file_listbox.curselection()
        # Retrieve the corresponding items
        selected_files = [file_listbox.get(i) for i in selected_indices]
        ignore_date = ignore_date_var.get()
        if not selected_files:
            messagebox.showwarning("Warning", "No files selected. Getting Status for ALL")
            selected_files = os.listdir(Config.WORK_FOLDER)
        else:
            if ignore_date:
                messagebox.showwarning(f"Warning", f"Calculating status for files[{selected_files}]")
            else:
                messagebox.showwarning(f"Warning", f"Calculating status for files[{selected_files}], ignoring updated")
            
        files_string, daily_export = Get_status(selected_files,ignore_date)
        status,file_name = Download_results(files_string, daily_export)
        # Display status in a pop-up or update a label
        status_label.config(text=f"Status: {status}")
        messagebox.showinfo("Status", f"System Status:\n{status}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to retrieve status: {e}")


# Function to download the result to a user-specified directory
def download_result():
    selected_indices = file_listbox.curselection()
    # Retrieve the corresponding items
    selected_files = [file_listbox.get(i) for i in selected_indices]
    
    if not selected_files:
        messagebox.showwarning("Warning", "No files selected. Getting Status for ALL")
        selected_files = os.listdir(Config.WORK_FOLDER)
        
    result_data, file_name = Download_results(selected_files)
    # Ask user to select directory to save the result file
    directory = filedialog.asksaveasfilename(initialfile=file_name,title="Select Directory to Save Result")
    if directory:
        # Example result content, replace with your actual result data
        
        
        try:
            result_data.to_csv(directory)
            messagebox.showinfo("Success", f"Results saved successfully at: {directory}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save the result file: {e}")
    else:
        messagebox.showwarning("No Directory Selected", "You must select a directory to save the result.")


# GUI Setup
root = tk.Tk()
root.title("File Management System")

# Frame for file operations
frame = tk.Frame(root)
frame.pack(pady=10)

# Browse and upload file
browse_button = tk.Button(frame, text="Browse and Load File", command=browse_file)
browse_button.grid(row=0, column=0, padx=5)

upload_button = tk.Button(frame, text="Upload Files", command=upload_files)
upload_button.grid(row=0, column=1, padx=5)

# File list and delete
file_listbox = Listbox(root,selectmode=tk.MULTIPLE, width=50, height=15)
file_listbox.pack(pady=10)
# Status Section
status_button = tk.Button(root, text="refresh", command=update_file_list)
status_button.pack(pady=5)

delete_button = tk.Button(root, text="Delete Selected File", command=delete_file)
delete_button.pack(pady=5)

status_frame = tk.Frame(root)
status_frame.pack()
# Status Section
status_button = tk.Button(status_frame, text="Get Status", command=get_status)
status_button.pack(side=tk.LEFT,pady=5)
#ignore last check date
# Create a BooleanVar for the Checkbutton
ignore_date_var = tk.BooleanVar()

# Create the Checkbutton
ignore_date_check = tk.Checkbutton(
    status_frame, text="Ignore Date", variable=ignore_date_var
)
ignore_date_check.pack(side=tk.LEFT, padx=5)


status_label = tk.Label(root, text="Status: Not Retrieved", fg="blue")
status_label.pack(pady=5)


# Download Result Section
download_button = tk.Button(root, text="Download Results", command=download_result)
download_button.pack(pady=5)

# Initialize file list
update_file_list()

# Run the GUI
root.mainloop()
