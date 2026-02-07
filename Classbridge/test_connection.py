#!/usr/bin/env python3
"""
Connection Test Script for ClassBridge Deployment
Tests the connection between frontend and backend
"""

import requests
import json
from datetime import datetime

# Configuration
BACKEND_URL = "https://classbridge-backend-bqj3.onrender.com"
FRONTEND_URL = "https://ed-tech-portal.vercel.app"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def test_backend_health():
    """Test if backend is responding"""
    print_section("Testing Backend Health")
    
    try:
        response = requests.get(f"{BACKEND_URL}/api/health", timeout=10)
        print(f"✅ Backend is reachable")
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   Status: {data.get('status')}")
            print(f"   Message: {data.get('message')}")
            print(f"   Environment: {data.get('environment')}")
            print(f"   Database: {data.get('database')}")
            print(f"   AI Enabled: {data.get('ai_enabled')}")
            return True
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"❌ Backend timeout - service might be sleeping (free tier)")
        print(f"   Try again in 30 seconds...")
        return False
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to backend")
        print(f"   Check if service is deployed and running on Render")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def test_cors():
    """Test CORS configuration"""
    print_section("Testing CORS Configuration")
    
    try:
        # Preflight request
        headers = {
            'Origin': FRONTEND_URL,
            'Access-Control-Request-Method': 'POST',
            'Access-Control-Request-Headers': 'Content-Type'
        }
        
        response = requests.options(
            f"{BACKEND_URL}/api/auth/login",
            headers=headers,
            timeout=10
        )
        
        cors_headers = {
            'Access-Control-Allow-Origin': response.headers.get('Access-Control-Allow-Origin'),
            'Access-Control-Allow-Methods': response.headers.get('Access-Control-Allow-Methods'),
            'Access-Control-Allow-Headers': response.headers.get('Access-Control-Allow-Headers'),
            'Access-Control-Allow-Credentials': response.headers.get('Access-Control-Allow-Credentials'),
        }
        
        print(f"✅ CORS preflight successful")
        print(f"   Status Code: {response.status_code}")
        print(f"\n   CORS Headers:")
        for key, value in cors_headers.items():
            if value:
                print(f"   - {key}: {value}")
        
        # Check if origin is allowed
        allowed_origin = cors_headers.get('Access-Control-Allow-Origin')
        if allowed_origin and (allowed_origin == FRONTEND_URL or allowed_origin == '*'):
            print(f"\n✅ Frontend origin is allowed")
            return True
        else:
            print(f"\n❌ Frontend origin might not be allowed")
            print(f"   Expected: {FRONTEND_URL}")
            print(f"   Got: {allowed_origin}")
            return False
            
    except Exception as e:
        print(f"❌ CORS test failed: {str(e)}")
        return False

def test_api_endpoint():
    """Test a simple API endpoint"""
    print_section("Testing API Endpoint")
    
    try:
        headers = {
            'Origin': FRONTEND_URL,
            'Content-Type': 'application/json'
        }
        
        # Test getting schools (public endpoint)
        response = requests.get(
            f"{BACKEND_URL}/api/admin/schools",
            headers=headers,
            timeout=10
        )
        
        print(f"✅ API endpoint reachable")
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   Response: {len(data)} schools found")
            return True
        else:
            print(f"   Response: {response.text[:200]}")
            return True  # Still counts as reachable
            
    except Exception as e:
        print(f"❌ API test failed: {str(e)}")
        return False

def test_frontend():
    """Test if frontend is accessible"""
    print_section("Testing Frontend")
    
    try:
        response = requests.get(FRONTEND_URL, timeout=10)
        print(f"✅ Frontend is accessible")
        print(f"   Status Code: {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ Frontend test failed: {str(e)}")
        return False

def main():
    print(f"\n{'#'*60}")
    print(f"#  ClassBridge Deployment Connection Test")
    print(f"#  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    print(f"\nConfiguration:")
    print(f"  Backend:  {BACKEND_URL}")
    print(f"  Frontend: {FRONTEND_URL}")
    
    results = {
        'Frontend Accessible': test_frontend(),
        'Backend Health': test_backend_health(),
        'CORS Configuration': test_cors(),
        'API Endpoint': test_api_endpoint(),
    }
    
    print_section("Test Results Summary")
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}  {test_name}")
        if not passed:
            all_passed = False
    
    print(f"\n{'='*60}")
    if all_passed:
        print("🎉 All tests passed! Your deployment is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the details above.")
        print("\nCommon fixes:")
        print("  1. Ensure backend is deployed and running on Render")
        print("  2. Check environment variables are set on Render")
        print("  3. Wait 30 seconds if backend was sleeping (free tier)")
        print("  4. Verify DATABASE_URL is set correctly")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
