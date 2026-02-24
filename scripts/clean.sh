docker compose down -v
rm -rf ../kafka/data/*
rm -rf ../minio/data*
#docker compose up -d
#docker logs -f kafka