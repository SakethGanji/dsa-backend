"""Service for data sampling with various strategies."""
import random
import math
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict

from src.core.abstractions.service_interfaces import ISamplingService
from src.core.abstractions.repositories import ITableReader


@dataclass
class SampleResult:
    """Result of a sampling operation."""
    data: List[Dict[str, Any]]
    sample_size: int
    total_size: int
    sampling_method: str
    metadata: Dict[str, Any]


class SamplingService(ISamplingService):
    """Service for sampling data using various strategies."""
    
    def __init__(self, table_reader: ITableReader):
        self._table_reader = table_reader
    
    async def sample_random(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        sample_size: int,
        seed: Optional[int] = None
    ) -> SampleResult:
        """Perform random sampling on a table."""
        # Set random seed for reproducibility
        if seed is not None:
            random.seed(seed)
        
        # Get total row count
        count_result = await self._table_reader.execute_query(
            dataset_id, commit_id,
            f"SELECT COUNT(*) FROM {table_name}"
        )
        total_rows = count_result.rows[0][0] if count_result.rows else 0
        
        if total_rows == 0:
            return SampleResult(
                data=[],
                sample_size=0,
                total_size=0,
                sampling_method='random',
                metadata={'seed': seed}
            )
        
        # Adjust sample size if larger than population
        actual_sample_size = min(sample_size, total_rows)
        
        # For small datasets, just get all rows and sample in memory
        if total_rows <= sample_size * 2:
            query = f"SELECT * FROM {table_name}"
            result = await self._table_reader.execute_query(dataset_id, commit_id, query)
            
            # Get schema for column names
            schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
            column_names = [col.name for col in schema.columns]
            
            # Convert to dictionaries
            all_data = [
                dict(zip(column_names, row))
                for row in result.rows
            ]
            
            # Random sample
            sampled_data = random.sample(all_data, actual_sample_size)
        else:
            # For large datasets, use database-level sampling
            # Using TABLESAMPLE for PostgreSQL (adjust for other databases)
            sample_percentage = (sample_size / total_rows) * 100
            query = f"""
                SELECT * FROM {table_name}
                TABLESAMPLE BERNOULLI ({sample_percentage})
                LIMIT {sample_size}
            """
            
            result = await self._table_reader.execute_query(dataset_id, commit_id, query)
            
            # Get schema for column names
            schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
            column_names = [col.name for col in schema.columns]
            
            # Convert to dictionaries
            sampled_data = [
                dict(zip(column_names, row))
                for row in result.rows
            ]
        
        return SampleResult(
            data=sampled_data,
            sample_size=len(sampled_data),
            total_size=total_rows,
            sampling_method='random',
            metadata={
                'seed': seed,
                'sampling_rate': len(sampled_data) / total_rows if total_rows > 0 else 0
            }
        )
    
    async def sample_stratified(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        stratify_column: str,
        sample_size: int,
        proportional: bool = True
    ) -> SampleResult:
        """Perform stratified sampling based on a categorical column."""
        # Get distribution of stratify column
        distribution_query = f"""
            SELECT {stratify_column}, COUNT(*) as count
            FROM {table_name}
            GROUP BY {stratify_column}
        """
        
        dist_result = await self._table_reader.execute_query(
            dataset_id, commit_id, distribution_query
        )
        
        # Calculate strata sizes
        strata_info = {}
        total_rows = 0
        for row in dist_result.rows:
            stratum_value = row[0]
            stratum_count = row[1]
            strata_info[stratum_value] = stratum_count
            total_rows += stratum_count
        
        if total_rows == 0:
            return SampleResult(
                data=[],
                sample_size=0,
                total_size=0,
                sampling_method='stratified',
                metadata={'stratify_column': stratify_column}
            )
        
        # Calculate samples per stratum
        samples_per_stratum = {}
        if proportional:
            # Proportional allocation
            for stratum, count in strata_info.items():
                proportion = count / total_rows
                stratum_sample_size = max(1, round(sample_size * proportion))
                samples_per_stratum[stratum] = min(stratum_sample_size, count)
        else:
            # Equal allocation
            equal_size = max(1, sample_size // len(strata_info))
            for stratum, count in strata_info.items():
                samples_per_stratum[stratum] = min(equal_size, count)
        
        # Get schema for column names
        schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
        column_names = [col.name for col in schema.columns]
        
        # Sample from each stratum
        sampled_data = []
        for stratum, stratum_sample_size in samples_per_stratum.items():
            # Use ORDER BY RANDOM() for sampling within stratum
            stratum_query = f"""
                SELECT * FROM {table_name}
                WHERE {stratify_column} = %s
                ORDER BY RANDOM()
                LIMIT {stratum_sample_size}
            """
            
            result = await self._table_reader.execute_query(
                dataset_id, commit_id, stratum_query, [stratum]
            )
            
            for row in result.rows:
                row_dict = dict(zip(column_names, row))
                sampled_data.append(row_dict)
        
        return SampleResult(
            data=sampled_data,
            sample_size=len(sampled_data),
            total_size=total_rows,
            sampling_method='stratified',
            metadata={
                'stratify_column': stratify_column,
                'proportional': proportional,
                'strata_counts': strata_info,
                'samples_per_stratum': samples_per_stratum
            }
        )
    
    async def sample_systematic(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        sample_size: int,
        order_by: Optional[str] = None
    ) -> SampleResult:
        """Perform systematic sampling (every nth row)."""
        # Get total row count
        count_result = await self._table_reader.execute_query(
            dataset_id, commit_id,
            f"SELECT COUNT(*) FROM {table_name}"
        )
        total_rows = count_result.rows[0][0] if count_result.rows else 0
        
        if total_rows == 0 or sample_size == 0:
            return SampleResult(
                data=[],
                sample_size=0,
                total_size=0,
                sampling_method='systematic',
                metadata={'interval': 0}
            )
        
        # Calculate sampling interval
        interval = max(1, total_rows // sample_size)
        
        # Build query with ROW_NUMBER
        order_clause = f"ORDER BY {order_by}" if order_by else ""
        query = f"""
            WITH numbered_rows AS (
                SELECT *, ROW_NUMBER() OVER ({order_clause}) as rn
                FROM {table_name}
            )
            SELECT * FROM numbered_rows
            WHERE MOD(rn - 1, {interval}) = 0
            LIMIT {sample_size}
        """
        
        result = await self._table_reader.execute_query(dataset_id, commit_id, query)
        
        # Get schema for column names (excluding the row number)
        schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
        column_names = [col.name for col in schema.columns]
        
        # Convert to dictionaries (excluding row number column)
        sampled_data = []
        for row in result.rows:
            # Remove the last column (row number)
            row_data = row[:-1]
            row_dict = dict(zip(column_names, row_data))
            sampled_data.append(row_dict)
        
        return SampleResult(
            data=sampled_data,
            sample_size=len(sampled_data),
            total_size=total_rows,
            sampling_method='systematic',
            metadata={
                'interval': interval,
                'order_by': order_by,
                'actual_interval': total_rows / len(sampled_data) if sampled_data else 0
            }
        )
    
    async def sample_cluster(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        cluster_column: str,
        num_clusters: int,
        samples_per_cluster: int
    ) -> SampleResult:
        """Perform cluster sampling."""
        # Get all unique clusters
        cluster_query = f"""
            SELECT DISTINCT {cluster_column}
            FROM {table_name}
            WHERE {cluster_column} IS NOT NULL
        """
        
        cluster_result = await self._table_reader.execute_query(
            dataset_id, commit_id, cluster_query
        )
        
        all_clusters = [row[0] for row in cluster_result.rows]
        
        if not all_clusters:
            return SampleResult(
                data=[],
                sample_size=0,
                total_size=0,
                sampling_method='cluster',
                metadata={'cluster_column': cluster_column}
            )
        
        # Randomly select clusters
        selected_clusters = random.sample(
            all_clusters,
            min(num_clusters, len(all_clusters))
        )
        
        # Get schema for column names
        schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
        column_names = [col.name for col in schema.columns]
        
        # Sample from selected clusters
        sampled_data = []
        cluster_sizes = {}
        
        for cluster in selected_clusters:
            # Get sample from this cluster
            cluster_sample_query = f"""
                SELECT * FROM {table_name}
                WHERE {cluster_column} = %s
                ORDER BY RANDOM()
                LIMIT {samples_per_cluster}
            """
            
            result = await self._table_reader.execute_query(
                dataset_id, commit_id, cluster_sample_query, [cluster]
            )
            
            cluster_sizes[cluster] = len(result.rows)
            
            for row in result.rows:
                row_dict = dict(zip(column_names, row))
                sampled_data.append(row_dict)
        
        # Get total row count
        count_result = await self._table_reader.execute_query(
            dataset_id, commit_id,
            f"SELECT COUNT(*) FROM {table_name}"
        )
        total_rows = count_result.rows[0][0] if count_result.rows else 0
        
        return SampleResult(
            data=sampled_data,
            sample_size=len(sampled_data),
            total_size=total_rows,
            sampling_method='cluster',
            metadata={
                'cluster_column': cluster_column,
                'num_clusters_requested': num_clusters,
                'num_clusters_selected': len(selected_clusters),
                'samples_per_cluster': samples_per_cluster,
                'selected_clusters': selected_clusters,
                'cluster_sizes': cluster_sizes
            }
        )
    
    async def get_sampling_recommendations(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        target_confidence: float = 0.95,
        margin_of_error: float = 0.05
    ) -> Dict[str, Any]:
        """Get recommendations for sample size based on statistical requirements."""
        # Get total row count
        count_result = await self._table_reader.execute_query(
            dataset_id, commit_id,
            f"SELECT COUNT(*) FROM {table_name}"
        )
        population_size = count_result.rows[0][0] if count_result.rows else 0
        
        if population_size == 0:
            return {
                'population_size': 0,
                'recommended_sample_size': 0,
                'confidence_level': target_confidence,
                'margin_of_error': margin_of_error
            }
        
        # Calculate sample size using Cochran's formula
        # For proportion estimation with finite population correction
        z_score = 1.96 if target_confidence == 0.95 else 2.576  # For 95% or 99% confidence
        p = 0.5  # Maximum variability
        
        # Initial sample size (infinite population)
        n0 = (z_score ** 2 * p * (1 - p)) / (margin_of_error ** 2)
        
        # Finite population correction
        recommended_size = int(n0 / (1 + (n0 - 1) / population_size))
        
        # Get schema info for stratification recommendations
        schema = await self._table_reader.get_table_schema(dataset_id, commit_id, table_name)
        categorical_columns = [
            col.name for col in schema.columns
            if 'varchar' in col.data_type.lower() or 'text' in col.data_type.lower()
        ]
        
        return {
            'population_size': population_size,
            'recommended_sample_size': recommended_size,
            'confidence_level': target_confidence,
            'margin_of_error': margin_of_error,
            'sampling_fraction': recommended_size / population_size,
            'stratification_candidates': categorical_columns[:5],  # Top 5 categorical columns
            'notes': [
                f"Sample size calculated for {target_confidence*100}% confidence level",
                f"Margin of error: ±{margin_of_error*100}%",
                "Consider stratified sampling if population has distinct subgroups"
            ]
        }