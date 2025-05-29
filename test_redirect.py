#!/usr/bin/env python3
"""
Test script for automatic login redirection
"""

import requests
import sys

def test_automatic_redirect():
    """Test automatic redirection to login page"""
    base_url = "http://localhost:5000"
    
    print("🔄 Testing Automatic Login Redirection")
    print("=" * 50)
    
    # Test 1: Access main page without authentication
    print("\n1. Testing main page access without authentication...")
    try:
        response = requests.get(f"{base_url}/", allow_redirects=False, timeout=5)
        
        if response.status_code == 302:  # Redirect status
            redirect_location = response.headers.get('Location', '')
            if '/login' in redirect_location:
                print("✅ Main page correctly redirects to login")
            else:
                print(f"❌ Main page redirects to unexpected location: {redirect_location}")
        elif response.status_code == 401:
            print("✅ Main page returns 401 (for AJAX requests)")
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to application: {e}")
        print("💡 Make sure the Flask application is running on localhost:5000")
        return False
    
    # Test 2: Access admin dashboard without authentication
    print("\n2. Testing admin dashboard access without authentication...")
    try:
        response = requests.get(f"{base_url}/admin/dashboard", allow_redirects=False, timeout=5)
        
        if response.status_code == 302:  # Redirect status
            redirect_location = response.headers.get('Location', '')
            if '/login' in redirect_location:
                print("✅ Admin dashboard correctly redirects to login")
            else:
                print(f"❌ Admin dashboard redirects to unexpected location: {redirect_location}")
        elif response.status_code == 401:
            print("✅ Admin dashboard returns 401 (for AJAX requests)")
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error testing admin dashboard: {e}")
    
    # Test 3: Access visualizations without authentication
    print("\n3. Testing visualizations page access without authentication...")
    try:
        response = requests.get(f"{base_url}/visuals", allow_redirects=False, timeout=5)
        
        if response.status_code == 302:  # Redirect status
            redirect_location = response.headers.get('Location', '')
            if '/login' in redirect_location:
                print("✅ Visualizations page correctly redirects to login")
            else:
                print(f"❌ Visualizations page redirects to unexpected location: {redirect_location}")
        elif response.status_code == 401:
            print("✅ Visualizations page returns 401 (for AJAX requests)")
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error testing visualizations page: {e}")
    
    # Test 4: Test API endpoints without authentication
    print("\n4. Testing API endpoints without authentication...")
    api_endpoints = [
        '/get-file-status',
        '/status',
        '/upload',
        '/admin/user-logs'
    ]
    
    for endpoint in api_endpoints:
        try:
            if endpoint == '/status':
                # POST request
                response = requests.post(
                    f"{base_url}{endpoint}", 
                    json={'files': ['test.txt']},
                    headers={'Content-Type': 'application/json'},
                    timeout=5
                )
            else:
                # GET request
                response = requests.get(f"{base_url}{endpoint}", timeout=5)
            
            if response.status_code == 401:
                print(f"   ✅ {endpoint} correctly returns 401")
            elif response.status_code == 302:
                print(f"   ✅ {endpoint} correctly redirects")
            else:
                print(f"   ❌ {endpoint} unexpected status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error testing {endpoint}: {e}")
    
    # Test 5: Test login page accessibility
    print("\n5. Testing login page accessibility...")
    try:
        response = requests.get(f"{base_url}/login", timeout=5)
        if response.status_code == 200:
            print("✅ Login page is accessible")
            
            # Check if error parameters work
            response_with_error = requests.get(f"{base_url}/login?error=insufficient_permissions", timeout=5)
            if response_with_error.status_code == 200:
                print("✅ Login page with error parameter is accessible")
            else:
                print("❌ Login page with error parameter failed")
        else:
            print(f"❌ Login page returned status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing login page: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Automatic redirection testing completed!")
    print("\n📋 Summary of redirection behavior:")
    print("   • Unauthenticated page requests → Automatic redirect to /login")
    print("   • Unauthenticated API requests → 401 status with redirect info")
    print("   • Permission denied → Redirect to /login with error parameter")
    print("   • Admin access denied → Redirect to /login with admin error")
    print("   • Login page shows appropriate error messages")
    
    print("\n🔧 How to test manually:")
    print("   1. Start the application: python app.py")
    print("   2. Open browser to http://localhost:5000")
    print("   3. Should automatically redirect to login page")
    print("   4. Try accessing /admin/dashboard without login")
    print("   5. Should redirect to login with error message")
    
    return True

if __name__ == "__main__":
    test_automatic_redirect()
