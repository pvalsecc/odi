VERSION=${1:-2.2.24}

docker stop testOrientdb
docker rm -f testOrientdb
docker volume rm testOrientDB

docker run -d --name testOrientdb \
  -v testOrientDB:/orientdb/databases \
  -v $PWD/db${VERSION}/config:/orientdb/config \
  -p 2424:2424 -p 2480:2480 \
  -e ORIENTDB_ROOT_PASSWORD=root \
  orientdb:${VERSION}

while ! curl http://localhost:2480/studio/index.html > /dev/null
do
    echo "Waiting for OrientDB to be up"
    sleep 1
done
