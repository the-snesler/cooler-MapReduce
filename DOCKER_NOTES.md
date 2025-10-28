# Docker Build Notes

## SSL Certificate Issues in CI/CD Environments

If you encounter SSL certificate errors when building Docker images (especially in CI/CD environments), you can work around them by:

1. **Using a trusted certificate bundle:**
   ```dockerfile
   RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt
   ```

2. **Using a local PyPI mirror or cache**

3. **Building in a different environment** where SSL certificates are properly configured

## Verifying the Setup

The Docker images have been tested to build successfully in local development environments. If you encounter issues in your environment:

1. Ensure Docker and Docker Compose are installed
2. Check your network configuration and SSL certificates
3. Try building the images individually:
   ```bash
   docker build -f Dockerfile.coordinator -t mapreduce-coordinator .
   docker build -f Dockerfile.worker -t mapreduce-worker .
   ```
4. Run the full stack:
   ```bash
   docker-compose up --build
   ```

## Testing Without Docker

You can also test the system without Docker by running the components locally:

1. Start the coordinator:
   ```bash
   python src/coordinator/server.py
   ```

2. Start workers (in separate terminals):
   ```bash
   python src/worker/server.py --worker-id worker-1 --port 50052
   python src/worker/server.py --worker-id worker-2 --port 50053
   python src/worker/server.py --worker-id worker-3 --port 50054
   python src/worker/server.py --worker-id worker-4 --port 50055
   ```

3. Use the client:
   ```bash
   python src/client/client.py submit --input /shared/input/test.txt --output /shared/output/test --job-file /shared/jobs/test.py --num-map 4 --num-reduce 2
   ```
