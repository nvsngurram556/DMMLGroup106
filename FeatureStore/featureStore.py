from feast import Entity, FeatureView, FeatureStore
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.value_type import ValueType
from feast.field import Field

# Define an entity for customers
customer = Entity(
    name="customerid",
    value_type=ValueType.STRING,
    description="Customer ID"
)

# Define the feature view for customer churn prediction
customer_features = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="churnscore", dtype=ValueType.FLOAT),
        Field(name="customer_tenure_months", dtype=ValueType.FLOAT),
    ],
    source=PostgreSQLSource(
        name="customer_features_source",
        query="""
        SELECT customerid, churnscore, customer_tenure_months, modifieddate
        FROM customers
        """,
        timestamp_field="modifieddate"
    )
)

# Query feature store for online inference
def retrieve_features(customer_id: int):
    store = FeatureStore(repo_path=".")
    features = store.get_online_features(
        features=["customer_features:churnscore", "customer_features:customer_tenure_months"],
        entity_rows=[{"customer_id": customer_id}]
    ).to_dict()
    return features

if __name__ == "__main__":
    customer_id = '8779-QRDMV'
    feature_values = retrieve_features(customer_id)
    print(f"Retrieved features for customer {customer_id}: {feature_values}")

# Feature Store Configuration (feature_store.yaml)
feature_store_yaml = """
project: my_feature_store
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
"""

with open("FeatureStore/feature_store.yaml", "w") as f:
    f.write(feature_store_yaml)

# Documentation of feature metadata
feature_metadata = """
Feature: churnscore
- Description: Likelihood of a customer churning
- Source: PostgreSQL (customers table)
- Version: v1.0
- Data Type: FLOAT

Feature: customer_tenure_months
- Description: Number of purchases made by the customer
- Source: PostgreSQL (customers table)
- Version: v1.0
- Data Type: FLOAT
"""

with open("FeatureStore/feature_metadata.txt", "w") as f:
    f.write(feature_metadata)