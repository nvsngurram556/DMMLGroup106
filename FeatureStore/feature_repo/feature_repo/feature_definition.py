# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String
from feast import FeatureService, FeatureView, Field, FileSource, ValueType

# Define an entity for the customer. You can think of an entity as a primary key used to
# fetch features.

customer = Entity(name="customer", join_keys = ["customer_ids"], value_type = ValueType.INT64, description = "ID of the Customer")

# Define a project for the feature repo
#project = Project(name="feature_repo", description="A project for Customer churn statistics")




# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
customer_stats_source = FileSource(
    path=r"data\predictors_df.parquet",
    timestamp_field="event_timestamp",
)

# Our parquet files contain sample data that includes a customer_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
customer_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="customer_df_feature_view",
    ttl=timedelta(seconds=86400*1),
    entities=[customer],
    
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="gender", dtype=String),
        Field(name="age", dtype=Float64),
        Field(name="satisfactionscore", dtype=Float64),
        Field(name="churnlabel", dtype=String),
        Field(name="monthlycharge", dtype=Float64),
        Field(name="avgmonthlygbdownload", dtype=Float64),
    ],
    online=True,
    source=customer_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={},
)

target_source = FileSource(
    path=r"data\target_df.parquet",
    timestamp_field="event_timestamp",
)


target_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="target_df_feature_view",
    entities=[customer],
    ttl=timedelta(seconds=86400*1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="churnlabel", dtype=String),
    ],
    online=True,
    source=target_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={},
)

