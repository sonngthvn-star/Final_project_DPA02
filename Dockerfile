# STEP 1: Start with the official Airflow image version defined in the .env
FROM apache/airflow:2.8.1

# STEP 2: Switch to 'root' user to install system-level software
USER root

# STEP 3: Install 'libpq-dev' and 'gcc'. 
# These are required to build the 'psycopg2' library which connects Python to Postgres.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# STEP 4: Switch back to the 'airflow' user for security best practices
USER airflow

# STEP 5: Copy your local 'requirements.txt' into the container
COPY requirements.txt .

# STEP 6: Install the Python libraries (Pandas, Loguru, etc.) into the image
RUN pip install --no-cache-dir -r requirements.txt