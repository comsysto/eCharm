"""Utility functions for testing."""

import pandas as pd


def verify_schema_follows(df: pd.DataFrame, predefined_schema: dict[str, str]) -> bool:
    """Check if the dataframe follows the `predefined_schema`."""
    # Extract schema of the DataFrame
    actual_df_schema = df.dtypes.apply(lambda x: x.name).to_dict()

    # Check if each field in the predefined schema is in the DF schema, but not vice versa,
    #   i.e. the DF schema can have additional (new) fields
    for field, dtype in predefined_schema.items():
        if field not in actual_df_schema or actual_df_schema[field] != dtype:
            return False

    return True
