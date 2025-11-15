# Start from the Spark image your AI assist found (apache/spark:3.4.1)
FROM apache/spark:3.4.1-python3

# Switch to the root user to install packages and fix permissions
USER root

# -----------------------------------------------------------------
# STEP 1: INSTALL JAVA JARS
# This solves the java.io.FileNotFoundException.
# -----------------------------------------------------------------
# Install wget and base utilities
RUN apt-get update && apt-get install -y \
	wget \
	gnupg \
	ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Place JARs in Spark's auto-load directory: /opt/spark/jars
# 1. Delta Lake Core JAR (for Scala 2.12, matches Spark 3.4.1)
RUN wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -P /opt/spark/jars/

# 2. Hadoop-AWS JAR (for S3A connectivity)
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/

# 3. AWS SDK Bundle (The critical dependency for hadoop-aws)
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars/

# 4. Delta Storage JAR (Required for Delta Lake 2.4.0+)
RUN wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -P /opt/spark/jars/

# -----------------------------------------------------------------
# STEP 1.5: INSTALL CHROME & SYSTEM DEPENDENCIES FOR KALEIDO/PNG EXPORTS
# -----------------------------------------------------------------
RUN apt-get update && apt-get install -y \
	fonts-liberation \
	libasound2 \
	libatk-bridge2.0-0 \
	libatk1.0-0 \
	libatspi2.0-0 \
	libc6 \
	libcairo2 \
	libcups2 \
	libdbus-1-3 \
	libexpat1 \
	libfontconfig1 \
	libgbm1 \
	libglib2.0-0 \
	libgtk-3-0 \
	libnspr4 \
	libnss3 \
	libpango-1.0-0 \
	libx11-6 \
	libx11-xcb1 \
	libxcb1 \
	libxcomposite1 \
	libxcursor1 \
	libxdamage1 \
	libxext6 \
	libxfixes3 \
	libxi6 \
	libxrandr2 \
	libxrender1 \
	libxss1 \
	libxtst6 \
	lsb-release \
	xdg-utils \
	&& rm -rf /var/lib/apt/lists/*

# Add Google Chrome repository and install Chrome (used by Kaleido's headless rendering backend)
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg && \
	echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
	apt-get update && apt-get install -y google-chrome-stable && \
	rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------
# STEP 2: INSTALL PYTHON PACKAGES (The "Venv" Fix)
# This solves the ModuleNotFoundError.
# -----------------------------------------------------------------
# Install python3-venv for virtual environment support
RUN apt-get update && apt-get install -y python3-venv && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY ./requirements.txt /tmp/requirements.txt

# Create a Python virtual environment
RUN python3 -m venv /opt/spark/venv

# Install all packages into that venv
RUN /opt/spark/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# -----------------------------------------------------------------
# STEP 3: PREPARE THE APP DIRECTORY
# -----------------------------------------------------------------
# Create the /app directory and set it as the working directory
WORKDIR /app
# We don't need to COPY ./app here, the volume mount will handle it.

# Switch back to the non-privileged spark user
USER $SPARK_UID
