up:
    pwd
    docker build -t se_migration_worker:latest ./../migration-worker
    docker compose up --detach --build

down:
    docker ps -q --filter "name=^/matrix-migration-worker" | xargs -r docker rm -f
    docker compose down
    docker volume rm controller_matrix-mongo_db-1
    docker volume rm controller_matrix-mongo_db-2
    docker volume rm controller_matrix-mongo_db-3
    docker volume rm controller_matrix-mongo_db-4

restart: down up

map:
 curl -v -f http://localhost:1234/mapping/startup

populate:
    ./populate-databases.sh

migrate from to url:
    curl -v -f 'http://localhost:1234/migrate?from={{from}}&to={{to}}&goal_url={{url}}'

get-state:
     curl -v -f http://localhost:1234/state

create_room name allowed_users:
    curl --request POST --url 'http://localhost:80/v1/addroom?=' --header 'Content-Type: application/json' --data '{"name": "{{name}}", "allowed_users": [{{allowed_users}}]}'

write msg room user:
    curl --request POST --url 'http://localhost:80/v1/sendmessage?=' --header 'Content-Type: application/json' --data '{"user": "{{user}}}}", "room": "{{room}}}}", "msg": "{{msg}}"}'

read room n:
    curl --request GET --url 'http://localhost:80/v1/post/{{room}}?n={{n}}':