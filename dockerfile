# Dockerfile

# Use the official AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.9

# Copy and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files to the Lambda task root
COPY . ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (e.g., main.lambda_handler)
CMD ["main.lambda_handler"]
