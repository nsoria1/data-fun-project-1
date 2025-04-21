# data-fun-project-1

## Kafka test
List topics:
docker-compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

Test schema registry:
curl -s http://localhost:8081/subjects | jq .
