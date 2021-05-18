To build an image (having dockerfile)
docker build -t nameOfImage:tag .

To run compose
docker-compose -f docker-compose-kafka.yml(or other docker compose) up

To kill a compose (after 2 ctrl+c)   
docker-compose -f docker-compose-kafka.yml(or other docker compose) down
