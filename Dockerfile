# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Set the working directory.
WORKDIR /app

# Copy the requirements file and install dependencies.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code.
COPY . .

# Expose the port your app runs on.
EXPOSE 8080

# Command to run the application.
CMD ["python", "app.py"]