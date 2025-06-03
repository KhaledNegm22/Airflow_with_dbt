FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Switch to root user to install packages
USER root

# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean

# Switch back to astro user (required by Astro)
USER astro

# Install dbt requirements
COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Copy your dbt project into the image
COPY my_dbt_project /usr/local/airflow/my_dbt_project