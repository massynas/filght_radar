docker-compose up --build
docker build -t spark-jupyter ./consumer_spark/.
docker run -p 8888:8888 -v $(pwd)/data:/home/jovyan/work spark-jupyter
