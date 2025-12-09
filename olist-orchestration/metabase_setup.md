# Docket + Metabaes + BiqQuery for Business Intelligence

We are using Metabase to deliver the business intelligence analysis.

Metabase is running in Docker and it accesses BigQuery dataset, created by previous dbt job.

## Docker Setup

Docker is an open platform for developing, shipping, and running applications. Docker enables you to separate your applications from your infrastructure so you can deliver software quickly. 

We use Docker, to run the Metabase application. Refer to [Docker](https://www.docker.com/) website for installation.

## Metabase Setup, Running and execute query

After Docker is ready, run it and get the latest Docker image:

```bash
docker pull metabase/metabase:latest
```

Then start the Metabase container:
```bash
docker run -d -p 4000:3000 --name metabase metabase/metabase
```

This will launch an Metabase server on port 4000 by default.

Once startup completes, you can access your Open Source Metabase at http://localhost:4000.

To run your Open Source Metabase on a different port, say port 12345:

```bash
docker run -d -p 12345:3000 --name metabase metabase/metabase
```

1. From Metabase GUI, follow the instructions to set up an admin account.

2. Adding a database connection

To add a database connection, click on the gear icon in the top right, and navigate to Admin settings > Databases > Add a database
```
  Database type: BigQuery
  Project ID: <your gcp project id>
  Service account JSON file: <upload your gcp credentials json file>
```
  Then click Save.

3. Adding a new question or SQL query

To add a new view, click on the +New icon in the top right, and select
```
  Question: graphical interface to configure a view 
  SQL query: directly post SQL command to generate the view
```

