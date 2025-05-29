#!/usr/bin/env python3
"""
Test script for the WDA Monitor authentication system
"""

import requests
import json
import sys

def test_authentication():
    """Test the authentication system"""
    base_url = "http://localhost:5000"
    
    print("🔧 Testing WDA Monitor Authentication System")
    print("=" * 50)
    
    # Test 1: Check if login page is accessible
    print("\n1. Testing login page accessibility...")
    try:
        response = requests.get(f"{base_url}/login", timeout=5)
        if response.status_code == 200:
            print("✅ Login page is accessible")
        else:
            print(f"❌ Login page returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to application: {e}")
        print("💡 Make sure the Flask application is running on localhost:5000")
        return False
    
    # Test 2: Check if main page redirects to login when not authenticated
    print("\n2. Testing authentication requirement...")
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 401 or "login" in response.url.lower():
            print("✅ Main page properly requires authentication")
        else:
            print(f"❌ Main page should require authentication but returned: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error testing main page: {e}")
    
    # Test 3: Test login with admin credentials
    print("\n3. Testing admin login...")
    session = requests.Session()
    login_data = {
        "username": "WDA",
        "password": "admin"
    }
    
    try:
        response = session.post(
            f"{base_url}/login",
            json=login_data,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('status') == 'success':
                print("✅ Admin login successful")
                user_info = result.get('user', {})
                print(f"   👤 User: {user_info.get('full_name')} ({user_info.get('role')})")
                print(f"   🔑 Permissions: {user_info.get('permissions')}")
            else:
                print(f"❌ Login failed: {result.get('message')}")
                return False
        else:
            print(f"❌ Login request failed with status: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error during login: {e}")
        return False
    
    # Test 4: Test access to main page after login
    print("\n4. Testing authenticated access...")
    try:
        response = session.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("✅ Main page accessible after authentication")
        else:
            print(f"❌ Main page not accessible after login: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing main page: {e}")
    
    # Test 5: Test admin dashboard access
    print("\n5. Testing admin dashboard access...")
    try:
        response = session.get(f"{base_url}/admin/dashboard", timeout=5)
        if response.status_code == 200:
            print("✅ Admin dashboard accessible")
        else:
            print(f"❌ Admin dashboard not accessible: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing admin dashboard: {e}")
    
    # Test 6: Test user activity logging
    print("\n6. Testing user activity logging...")
    try:
        response = session.get(f"{base_url}/admin/user-logs", timeout=5)
        if response.status_code == 200:
            logs = response.json().get('logs', [])
            print(f"✅ User activity logging working ({len(logs)} log entries)")
            if logs:
                latest_log = logs[0]
                print(f"   📝 Latest activity: {latest_log.get('action')} by {latest_log.get('username')}")
        else:
            print(f"❌ Cannot access user logs: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing user logs: {e}")
    
    # Test 7: Test logout
    print("\n7. Testing logout...")
    try:
        response = session.post(f"{base_url}/logout", timeout=5)
        if response.status_code == 200:
            print("✅ Logout successful")
        else:
            print(f"❌ Logout failed: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error during logout: {e}")
    
    # Test 8: Test different user roles
    print("\n8. Testing different user roles...")
    test_users = [
        {"username": "analyst1", "password": "analyst123", "role": "analyst"},
        {"username": "operator1", "password": "operator123", "role": "operator"},
        {"username": "viewer1", "password": "viewer123", "role": "viewer"},
        {"username": "manager1", "password": "manager123", "role": "manager"}
    ]
    
    for user in test_users:
        try:
            new_session = requests.Session()
            response = new_session.post(
                f"{base_url}/login",
                json={"username": user["username"], "password": user["password"]},
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    print(f"   ✅ {user['username']} ({user['role']}) login successful")
                else:
                    print(f"   ❌ {user['username']} login failed: {result.get('message')}")
            else:
                print(f"   ❌ {user['username']} login failed with status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error testing {user['username']}: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Authentication system testing completed!")
    print("\n📋 Summary of implemented features:")
    print("   • Multi-user authentication with role-based access control")
    print("   • 7 different user accounts with varying permissions")
    print("   • Admin dashboard with user activity logging")
    print("   • Session management and secure logout")
    print("   • File status checking with concurrent access prevention")
    print("   • User activity tracking for all actions")
    
    print("\n👥 Available Users:")
    print("   • WDA (admin) - password: admin")
    print("   • analyst1/analyst2 - password: analyst123/analyst456")
    print("   • operator1/operator2 - password: operator123/operator456")
    print("   • viewer1 - password: viewer123")
    print("   • manager1 - password: manager123")
    
    return True

if __name__ == "__main__":
    test_authentication()
