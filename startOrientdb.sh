docker stop testOrientdb
docker rm -f testOrientdb
docker volume rm testOrientDB
docker run -d --name testOrientdb -v testOrientDB:/orientdb/databases -p 2424:2424 -p 2480:2480 -v $PWD/db/config:/orientdb/config -e ORIENTDB_ROOT_PASSWORD=root orientdb:2.2.24
