"""
Pipeline Sampling Examples

This module demonstrates various pipeline sampling configurations.
"""

# Example 1: Filter -> Random Sample
filter_then_random = {
    "pipeline": [
        {
            "step": "filter",
            "parameters": {
                "conditions": [
                    {"column": "status", "operator": "=", "value": "active"},
                    {"column": "score", "operator": ">", "value": 80}
                ],
                "logic": "AND"
            }
        },
        {
            "step": "random_sample",
            "parameters": {
                "sample_size": 1000,
                "seed": 42
            }
        }
    ],
    "output_name": "filtered_random_sample",
    "selection": {
        "columns": ["id", "name", "status", "score"],
        "order_by": "score",
        "order_desc": True
    }
}

# Example 2: Filter -> Stratified Sample -> Random Sample
multi_stage_pipeline = {
    "pipeline": [
        {
            "step": "filter",
            "parameters": {
                "conditions": [
                    {"column": "created_date", "operator": ">=", "value": "2024-01-01"}
                ],
                "logic": "AND"
            }
        },
        {
            "step": "stratified_sample",
            "parameters": {
                "strata_columns": ["region", "category"],
                "sample_size": 0.5,  # 50% from each stratum
                "seed": 42
            }
        },
        {
            "step": "random_sample",
            "parameters": {
                "sample_size": 0.1  # 10% of the stratified result
            }
        }
    ],
    "output_name": "complex_pipeline_sample"
}

# Example 3: Cluster -> Consecutive -> Filter
cluster_consecutive_pipeline = {
    "pipeline": [
        {
            "step": "cluster_sample",
            "parameters": {
                "cluster_column": "department",
                "num_clusters": 5,
                "sample_within_clusters": False
            }
        },
        {
            "step": "consecutive_sample",
            "parameters": {
                "interval": 10,
                "start": 0
            }
        },
        {
            "step": "filter",
            "parameters": {
                "conditions": [
                    {"column": "salary", "operator": ">", "value": 50000}
                ],
                "logic": "AND"
            }
        }
    ],
    "output_name": "cluster_consecutive_filtered"
}

# Example 4: All steps combined
all_steps_pipeline = {
    "pipeline": [
        # Step 1: Initial filtering
        {
            "step": "filter",
            "parameters": {
                "conditions": [
                    {"column": "status", "operator": "IN", "value": ["active", "pending"]},
                    {"column": "created_year", "operator": ">=", "value": 2023}
                ],
                "logic": "AND"
            }
        },
        # Step 2: Stratified sampling by region
        {
            "step": "stratified_sample",
            "parameters": {
                "strata_columns": ["region"],
                "sample_size": 0.8,  # 80% from each region
                "seed": 123
            }
        },
        # Step 3: Cluster sampling by department
        {
            "step": "cluster_sample",
            "parameters": {
                "cluster_column": "department",
                "num_clusters": 10,
                "sample_within_clusters": True  # Also sample within clusters
            }
        },
        # Step 4: Consecutive sampling (every 5th record)
        {
            "step": "consecutive_sample",
            "parameters": {
                "interval": 5,
                "start": 0
            }
        },
        # Step 5: Final random sampling
        {
            "step": "random_sample",
            "parameters": {
                "sample_size": 500,  # Final 500 records
                "seed": 456
            }
        }
    ],
    "output_name": "comprehensive_pipeline_sample",
    "selection": {
        "columns": ["id", "region", "department", "status", "value"],
        "order_by": "value",
        "order_desc": True,
        "limit": 100  # Return only top 100 records
    }
}

# Example 5: Percentage-based sampling pipeline
percentage_pipeline = {
    "pipeline": [
        {
            "step": "random_sample",
            "parameters": {
                "sample_size": 0.5  # 50% of data
            }
        },
        {
            "step": "stratified_sample",
            "parameters": {
                "strata_columns": ["category"],
                "sample_size": 0.2  # 20% from each stratum
            }
        },
        {
            "step": "random_sample",
            "parameters": {
                "sample_size": 0.5  # 50% of previous result
            }
        }
    ],
    "output_name": "percentage_based_sample"
}

# Traditional (non-pipeline) example for comparison
traditional_example = {
    "method": "stratified",
    "parameters": {
        "strata_columns": ["region", "category"],
        "sample_size": 1000,
        "min_per_stratum": 10,
        "seed": 42
    },
    "output_name": "traditional_stratified_sample",
    "filters": {
        "conditions": [
            {"column": "status", "operator": "=", "value": "active"}
        ],
        "logic": "AND"
    },
    "selection": {
        "columns": ["id", "region", "category", "value"],
        "order_by": "value",
        "order_desc": True
    }
}

if __name__ == "__main__":
    import json
    
    print("Pipeline Sampling Examples\n")
    print("=" * 50)
    
    examples = [
        ("Filter -> Random Sample", filter_then_random),
        ("Multi-Stage Pipeline", multi_stage_pipeline),
        ("Cluster -> Consecutive -> Filter", cluster_consecutive_pipeline),
        ("All Steps Combined", all_steps_pipeline),
        ("Percentage-Based Pipeline", percentage_pipeline),
        ("Traditional (Non-Pipeline)", traditional_example)
    ]
    
    for name, config in examples:
        print(f"\n{name}:")
        print(json.dumps(config, indent=2))
        print("-" * 50)