# Cloud-Native Transaction Data Pipeline (AWS)

This project implements a cloud-native data pipeline that ingests, validates, transforms, aggregates, and uploads transactional data to Amazon S3 using a partitioned data lake structure.

It is designed to simulate production-grade data engineering patterns, including:

Idempotent pipeline execution

Schema validation and data quality controls

Clean vs rejected record handling

Date-based partitioning (data lake layout)

Daily aggregation layer

Environment-driven configuration

S3 integration with preserved partition structure

The project demonstrates both data engineering and cloud engineering principles.