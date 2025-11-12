docker compose down -v
rm -rf ./kafka/data/*
docker compose up -d
docker logs -f kafka
