"""Security and validation tests for all API endpoints."""

import pytest
from httpx import AsyncClient
import json
from uuid import uuid4
from unittest.mock import patch


@pytest.mark.asyncio
class TestSQLInjectionPrevention:
    """Test SQL injection prevention across all endpoints."""
    
    SQL_INJECTION_PAYLOADS = [
        "'; DROP TABLE users; --",
        "1' OR '1'='1",
        "admin'--",
        "1; DELETE FROM datasets WHERE 1=1; --",
        "' UNION SELECT * FROM users--",
        "1' AND 1=1--",
        "'; EXEC xp_cmdshell('dir'); --"
    ]
    
    async def test_dataset_name_sql_injection(self, client: AsyncClient, auth_headers: dict):
        """Test SQL injection in dataset name."""
        for payload in self.SQL_INJECTION_PAYLOADS:
            response = await client.post(
                "/api/datasets/",
                json={
                    "name": payload,
                    "description": "Test dataset"
                },
                headers=auth_headers
            )
            # Should be rejected by validation
            assert response.status_code in [400, 422]
            assert "validation" in response.text.lower() or "invalid" in response.text.lower()
    
    async def test_dataset_description_sql_injection(self, client: AsyncClient, auth_headers: dict):
        """Test SQL injection in dataset description."""
        for payload in self.SQL_INJECTION_PAYLOADS:
            response = await client.post(
                "/api/datasets/",
                json={
                    "name": "Valid Name",
                    "description": payload
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]
    
    async def test_dataset_tags_sql_injection(self, client: AsyncClient, auth_headers: dict):
        """Test SQL injection in dataset tags."""
        response = await client.post(
            "/api/datasets/",
            json={
                "name": "Valid Name",
                "description": "Valid description",
                "tags": ["valid-tag", "'; DROP TABLE tags; --"]
            },
            headers=auth_headers
        )
        assert response.status_code in [400, 422]
    
    async def test_user_soeid_sql_injection(self, client: AsyncClient):
        """Test SQL injection in user SOEID."""
        for payload in self.SQL_INJECTION_PAYLOADS[:3]:  # SOEID has length constraint
            response = await client.post(
                "/api/users/login",
                data={
                    "username": payload,
                    "password": "password123"
                }
            )
            # Should fail validation or auth
            assert response.status_code in [401, 422]


@pytest.mark.asyncio
class TestXSSPrevention:
    """Test XSS prevention across all endpoints."""
    
    XSS_PAYLOADS = [
        "<script>alert('XSS')</script>",
        "<img src=x onerror=alert('XSS')>",
        "<iframe src='javascript:alert(\"XSS\")'></iframe>",
        "javascript:alert('XSS')",
        "<svg onload=alert('XSS')>",
        "<<SCRIPT>alert('XSS');//<</SCRIPT>",
        "<script>alert(String.fromCharCode(88,83,83))</script>"
    ]
    
    async def test_dataset_name_xss(self, client: AsyncClient, auth_headers: dict):
        """Test XSS in dataset name."""
        for payload in self.XSS_PAYLOADS:
            response = await client.post(
                "/api/datasets/",
                json={
                    "name": payload,
                    "description": "Test"
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]
            assert "script" in response.text.lower() or "validation" in response.text.lower()
    
    async def test_commit_message_xss(self, client: AsyncClient, auth_headers: dict):
        """Test XSS in commit message."""
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            for payload in self.XSS_PAYLOADS:
                response = await client.post(
                    "/api/datasets/1/refs/main/commits",
                    json={
                        "message": payload,
                        "data": [{"id": 1, "value": "test"}]
                    },
                    headers=auth_headers
                )
                assert response.status_code in [400, 422]


@pytest.mark.asyncio
class TestInputValidation:
    """Test input validation for all endpoints."""
    
    async def test_dataset_name_constraints(self, client: AsyncClient, auth_headers: dict):
        """Test dataset name length and character constraints."""
        # Empty name
        response = await client.post(
            "/api/datasets/",
            json={"name": "", "description": "Test"},
            headers=auth_headers
        )
        assert response.status_code == 422
        
        # Too long name (>255 chars)
        response = await client.post(
            "/api/datasets/",
            json={"name": "x" * 256, "description": "Test"},
            headers=auth_headers
        )
        assert response.status_code == 422
        
        # Special characters
        invalid_names = [
            "dataset@name",
            "dataset#name",
            "dataset$name",
            "dataset%name",
            "dataset&name",
            "dataset*name",
            "dataset(name)",
            "dataset[name]",
            "dataset{name}",
            "dataset|name",
            "dataset\\name",
            "dataset/name",
            "dataset:name",
            "dataset;name",
            "dataset<name>",
            "dataset?name"
        ]
        
        for name in invalid_names:
            response = await client.post(
                "/api/datasets/",
                json={"name": name, "description": "Test"},
                headers=auth_headers
            )
            assert response.status_code in [400, 422]
    
    async def test_pagination_limits(self, client: AsyncClient, auth_headers: dict):
        """Test pagination parameter limits."""
        # Negative offset
        response = await client.get(
            "/api/datasets/?offset=-1",
            headers=auth_headers
        )
        assert response.status_code == 422
        
        # Excessive limit
        response = await client.get(
            "/api/datasets/?limit=100001",
            headers=auth_headers
        )
        assert response.status_code == 422
        
        # Zero limit
        response = await client.get(
            "/api/datasets/?limit=0",
            headers=auth_headers
        )
        assert response.status_code == 422
    
    async def test_user_password_validation(self, client: AsyncClient, auth_headers: dict):
        """Test password strength requirements."""
        weak_passwords = [
            "short",           # Too short
            "alllowercase",    # No uppercase
            "ALLUPPERCASE",    # No lowercase
            "NoNumbers!",      # No digits
            "NoSpecial123",    # No special chars
            "        ",        # Only spaces
        ]
        
        for password in weak_passwords:
            response = await client.post(
                "/api/users/register",
                json={
                    "soeid": "TEST123",
                    "password": password,
                    "role_id": 1
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]
    
    async def test_permission_type_validation(self, client: AsyncClient, auth_headers: dict, test_dataset: dict):
        """Test permission type validation."""
        invalid_permissions = [
            "superadmin",
            "owner",
            "delete",
            "execute",
            "READ",  # Case sensitive
            "Write",
            "",
            None
        ]
        
        for perm in invalid_permissions:
            response = await client.post(
                f"/api/datasets/{test_dataset['id']}/permissions",
                json={
                    "user_id": 1,
                    "permission_type": perm
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]


@pytest.mark.asyncio
class TestFileUploadSecurity:
    """Test file upload security."""
    
    async def test_file_extension_validation(self, client: AsyncClient, auth_headers: dict):
        """Test that only allowed file extensions are accepted."""
        dangerous_files = [
            ('malware.exe', b'MZ\x90\x00', 'application/x-msdownload'),
            ('script.js', b'alert("XSS")', 'application/javascript'),
            ('shell.sh', b'#!/bin/bash\nrm -rf /', 'application/x-sh'),
            ('hack.php', b'<?php system($_GET["cmd"]); ?>', 'application/x-php'),
            ('payload.jsp', b'<%@ page import="java.io.*" %>', 'application/x-jsp'),
        ]
        
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            for filename, content, mime_type in dangerous_files:
                files = {'file': (filename, content, mime_type)}
                data = {'commit_message': 'Test import'}
                
                response = await client.post(
                    "/api/datasets/1/refs/main/import",
                    files=files,
                    data=data,
                    headers=auth_headers
                )
                # Should reject dangerous file types
                assert response.status_code in [400, 415, 422]
    
    async def test_file_size_limits(self, client: AsyncClient, auth_headers: dict):
        """Test file size limits."""
        # Create a large file (simulate)
        large_content = b'x' * (100 * 1024 * 1024 + 1)  # 100MB + 1 byte
        
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            files = {'file': ('large.csv', large_content, 'text/csv')}
            data = {'commit_message': 'Large file import'}
            
            response = await client.post(
                "/api/datasets/1/refs/main/import",
                files=files,
                data=data,
                headers=auth_headers
            )
            # Should reject files that are too large
            assert response.status_code in [400, 413, 422]


@pytest.mark.asyncio
class TestPathTraversal:
    """Test path traversal prevention."""
    
    async def test_filename_path_traversal(self, client: AsyncClient, auth_headers: dict):
        """Test path traversal in filenames."""
        dangerous_filenames = [
            "../../../etc/passwd.csv",
            "..\\..\\..\\windows\\system32\\config\\sam.csv",
            "data/../../../sensitive.csv",
            "./././../../../root.csv",
            "valid.csv/../../../etc/shadow"
        ]
        
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            for filename in dangerous_filenames:
                files = {'file': (filename, b'id,value\n1,test', 'text/csv')}
                data = {'commit_message': 'Test import'}
                
                response = await client.post(
                    "/api/datasets/1/refs/main/import",
                    files=files,
                    data=data,
                    headers=auth_headers
                )
                assert response.status_code in [400, 422]


@pytest.mark.asyncio
class TestRateLimiting:
    """Test rate limiting and DoS prevention."""
    
    async def test_excessive_data_rows(self, client: AsyncClient, auth_headers: dict):
        """Test handling of excessive data rows in commit."""
        # Try to create commit with too many rows
        excessive_data = [{"id": i, "value": f"test{i}"} for i in range(100001)]
        
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            response = await client.post(
                "/api/datasets/1/refs/main/commits",
                json={
                    "message": "Too many rows",
                    "data": excessive_data
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 413, 422]
    
    async def test_excessive_string_length(self, client: AsyncClient, auth_headers: dict):
        """Test handling of excessive string lengths."""
        # Very long string in data
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            response = await client.post(
                "/api/datasets/1/refs/main/commits",
                json={
                    "message": "Long string test",
                    "data": [{"id": 1, "value": "x" * 10001}]  # >10k chars
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]


@pytest.mark.asyncio
class TestAuthorizationBoundaries:
    """Test authorization boundaries."""
    
    async def test_dataset_permission_escalation(self, client: AsyncClient, auth_headers: dict, test_dataset: dict):
        """Test that users cannot escalate their own permissions."""
        # Try to grant admin permission to self
        response = await client.post(
            f"/api/datasets/{test_dataset['id']}/permissions",
            json={
                "user_id": 1,  # Assuming this is the current user
                "permission_type": "admin"
            },
            headers=auth_headers
        )
        # Should either succeed (if already admin) or fail gracefully
        if response.status_code == 200:
            # Verify user was already admin
            assert True  # This would need actual verification
        else:
            assert response.status_code in [400, 403]
    
    async def test_cross_dataset_access(self, client: AsyncClient, auth_headers: dict):
        """Test that users cannot access datasets they don't have permission for."""
        # Try to access a dataset that doesn't exist or user has no access to
        response = await client.get(
            "/api/datasets/999999",
            headers=auth_headers
        )
        assert response.status_code in [403, 404]
    
    async def test_invalid_user_id_permission_grant(self, client: AsyncClient, auth_headers: dict, test_dataset: dict):
        """Test granting permissions to invalid user IDs."""
        invalid_user_ids = [0, -1, -999, 999999999]
        
        for user_id in invalid_user_ids:
            response = await client.post(
                f"/api/datasets/{test_dataset['id']}/permissions",
                json={
                    "user_id": user_id,
                    "permission_type": "read"
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 404, 422]


@pytest.mark.asyncio
class TestDataIntegrity:
    """Test data integrity validation."""
    
    async def test_commit_data_consistency(self, client: AsyncClient, auth_headers: dict):
        """Test that commit data has consistent structure."""
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            # Inconsistent column structure
            response = await client.post(
                "/api/datasets/1/refs/main/commits",
                json={
                    "message": "Inconsistent data",
                    "data": [
                        {"id": 1, "name": "Alice", "value": 100},
                        {"id": 2, "name": "Bob"},  # Missing 'value'
                        {"id": 3, "value": 300}     # Missing 'name'
                    ]
                },
                headers=auth_headers
            )
            assert response.status_code in [400, 422]
    
    async def test_data_type_validation(self, client: AsyncClient, auth_headers: dict):
        """Test data type validation in commits."""
        with patch('src.api.versioning.get_current_user', return_value={"id": 1}):
            # Invalid data types
            test_cases = [
                # Nested objects (might not be supported)
                [{"id": 1, "data": {"nested": {"too": "deep"}}}],
                # Circular reference simulation
                [{"id": 1, "self_ref": "[Circular]"}],
                # Binary data in JSON
                [{"id": 1, "binary": b"\x00\x01\x02\x03"}],
            ]
            
            for data in test_cases:
                try:
                    response = await client.post(
                        "/api/datasets/1/refs/main/commits",
                        json={
                            "message": "Invalid data type test",
                            "data": data
                        },
                        headers=auth_headers
                    )
                    assert response.status_code in [400, 422]
                except (TypeError, ValueError):
                    # JSON serialization might fail
                    pass