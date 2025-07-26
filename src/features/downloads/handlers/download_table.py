"""Handler for downloading individual tables from datasets."""

import io
import csv
import json
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ..models import DownloadTableCommand


class DownloadTableHandler:
    """Handler for downloading table data."""
    
    def __init__(self, uow: PostgresUnitOfWork, metadata_reader: PostgresTableReader, data_reader: PostgresTableReader):
        self._uow = uow
        self._metadata_reader = metadata_reader
        self._data_reader = data_reader
    
    async def handle(self, command: DownloadTableCommand) -> Dict[str, Any]:
        """
        Download table data in specified format.
        
        Returns dict with:
        - content: file content (bytes)
        - content_type: MIME type
        - filename: suggested filename
        """
        # Validate format
        if command.format not in ["csv", "json"]:
            raise ValidationException(f"Unsupported format: {command.format}", field="format")
        
        # Get dataset info
        dataset = await self._uow.datasets.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", command.ref_name)
        
        commit_id = ref['commit_id']
        
        # Get all data in batches
        all_data = []
        offset = 0
        batch_size = 1000
        
        while True:
            result = await self._data_reader.get_table_data(
                commit_id=commit_id,
                table_key=command.table_key,
                offset=offset,
                limit=batch_size
            )
            
            # Filter columns if specified
            if command.columns and result:
                filtered_result = []
                for row in result:
                    # Keep _logical_row_id plus requested columns
                    filtered_row = {k: v for k, v in row.items() 
                                  if k in command.columns or k == '_logical_row_id'}
                    filtered_result.append(filtered_row)
                result = filtered_result
            
            all_data.extend(result)
            offset += batch_size
            
            if len(result) < batch_size:
                break
        
        # Format data based on requested format
        if command.format == "csv":
            return await self._format_as_csv(
                data=all_data,
                dataset_name=dataset['name'],
                table_key=command.table_key,
                commit_id=commit_id
            )
        else:  # json
            return await self._format_as_json(
                data=all_data,
                dataset_name=dataset['name'],
                table_key=command.table_key,
                commit_id=commit_id,
                metadata_reader=self._metadata_reader
            )
    
    async def _format_as_csv(
        self, 
        data: List[Dict], 
        dataset_name: str,
        table_key: str,
        commit_id: str
    ) -> Dict[str, Any]:
        """Format data as CSV."""
        output = io.StringIO()
        
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        filename = f"{dataset_name}_{table_key}_{commit_id[:8]}.csv"
        
        return {
            "content": output.getvalue().encode('utf-8'),
            "content_type": "text/csv",
            "filename": filename
        }
    
    async def _format_as_json(
        self,
        data: List[Dict],
        dataset_name: str,
        table_key: str,
        commit_id: str,
        metadata_reader: PostgresTableReader
    ) -> Dict[str, Any]:
        """Format data as JSON with schema."""
        # Get schema
        schema = await metadata_reader.get_table_schema(commit_id, table_key)
        
        result = {
            "dataset_name": dataset_name,
            "commit_id": commit_id,
            "table_key": table_key,
            "schema": schema,
            "row_count": len(data),
            "data": data
        }
        
        filename = f"{dataset_name}_{table_key}_{commit_id[:8]}.json"
        
        return {
            "content": json.dumps(result, indent=2, default=str).encode('utf-8'),
            "content_type": "application/json",
            "filename": filename
        }