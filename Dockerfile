FROM python:3.11-slim
# Set the working directory in the container.
WORKDIR /app

# Copy the requirements file in the container and install dependencies.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code in teh container /app
COPY . .

# Expose the port the app runs on.
EXPOSE 8080

# Command to run the application.
CMD ["python", "app.py"]
