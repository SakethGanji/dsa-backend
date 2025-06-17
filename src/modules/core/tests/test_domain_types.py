import pytest
from pathlib import Path

from ..domain.types import (
    ContentHash, FilePath, Permission, FileType,
    StorageType, DatasetStatus, VersionStatus,
    SamplingMethod, CompressionType
)


class TestContentHash:
    """Test ContentHash value object."""
    
    def test_valid_content_hash(self):
        """Test creating a valid content hash."""
        valid_hash = "a" * 64
        hash_obj = ContentHash(value=valid_hash)
        assert hash_obj.value == valid_hash
        assert str(hash_obj) == valid_hash
    
    def test_invalid_length(self):
        """Test content hash with invalid length."""
        with pytest.raises(ValueError, match="must be a 64-character"):
            ContentHash(value="a" * 63)
        
        with pytest.raises(ValueError, match="must be a 64-character"):
            ContentHash(value="a" * 65)
    
    def test_invalid_characters(self):
        """Test content hash with invalid characters."""
        with pytest.raises(ValueError, match="valid hexadecimal"):
            ContentHash(value="g" * 64)  # 'g' is not a hex character
        
        with pytest.raises(ValueError, match="valid hexadecimal"):
            ContentHash(value="z" * 64)
    
    def test_case_insensitive(self):
        """Test that hash validation is case-insensitive."""
        upper_hash = "A" * 64
        lower_hash = "a" * 64
        
        hash1 = ContentHash(value=upper_hash)
        hash2 = ContentHash(value=lower_hash)
        
        assert hash1.value == upper_hash
        assert hash2.value == lower_hash
    
    def test_equality(self):
        """Test content hash equality."""
        hash1 = ContentHash(value="a" * 64)
        hash2 = ContentHash(value="a" * 64)
        hash3 = ContentHash(value="b" * 64)
        
        assert hash1 == hash2
        assert hash1 != hash3


class TestFilePath:
    """Test FilePath value object."""
    
    def test_valid_file_path(self):
        """Test creating a valid file path."""
        path_str = "/data/datasets/file.parquet"
        file_path = FilePath(value=path_str)
        
        assert file_path.value == path_str
        assert str(file_path) == path_str
        assert isinstance(file_path.path, Path)
        assert file_path.name == "file.parquet"
        assert file_path.suffix == ".parquet"
    
    def test_empty_path(self):
        """Test that empty paths are rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            FilePath(value="")
    
    def test_path_properties(self):
        """Test file path properties."""
        file_path = FilePath(value="/data/datasets/subfolder/data.csv")
        
        assert file_path.path == Path("/data/datasets/subfolder/data.csv")
        assert file_path.name == "data.csv"
        assert file_path.suffix == ".csv"
    
    def test_equality(self):
        """Test file path equality."""
        path1 = FilePath(value="/data/file1.txt")
        path2 = FilePath(value="/data/file1.txt")
        path3 = FilePath(value="/data/file2.txt")
        
        assert path1 == path2
        assert path1 != path3


class TestEnums:
    """Test enum types."""
    
    def test_permission_enum(self):
        """Test Permission enum."""
        assert Permission.READ.value == "read"
        assert Permission.WRITE.value == "write"
        assert Permission.ADMIN.value == "admin"
    
    def test_file_type_enum(self):
        """Test FileType enum."""
        assert FileType.CSV.value == "csv"
        assert FileType.PARQUET.value == "parquet"
        assert FileType.EXCEL.value == "excel"
        assert FileType.JSON.value == "json"
        assert FileType.TEXT.value == "text"
        assert FileType.BINARY.value == "binary"
    
    def test_storage_type_enum(self):
        """Test StorageType enum."""
        assert StorageType.LOCAL.value == "local"
        assert StorageType.S3.value == "s3"
        assert StorageType.AZURE.value == "azure"
        assert StorageType.GCS.value == "gcs"
    
    def test_dataset_status_enum(self):
        """Test DatasetStatus enum."""
        assert DatasetStatus.DRAFT.value == "draft"
        assert DatasetStatus.ACTIVE.value == "active"
        assert DatasetStatus.ARCHIVED.value == "archived"
        assert DatasetStatus.DELETED.value == "deleted"
    
    def test_version_status_enum(self):
        """Test VersionStatus enum."""
        assert VersionStatus.CREATING.value == "creating"
        assert VersionStatus.READY.value == "ready"
        assert VersionStatus.FAILED.value == "failed"
        assert VersionStatus.ARCHIVED.value == "archived"
    
    def test_sampling_method_enum(self):
        """Test SamplingMethod enum."""
        assert SamplingMethod.RANDOM.value == "random"
        assert SamplingMethod.STRATIFIED.value == "stratified"
        assert SamplingMethod.SYSTEMATIC.value == "systematic"
        assert SamplingMethod.CLUSTER.value == "cluster"
    
    def test_compression_type_enum(self):
        """Test CompressionType enum."""
        assert CompressionType.NONE.value == "none"
        assert CompressionType.GZIP.value == "gzip"
        assert CompressionType.SNAPPY.value == "snappy"
        assert CompressionType.LZ4.value == "lz4"
        assert CompressionType.ZSTD.value == "zstd"