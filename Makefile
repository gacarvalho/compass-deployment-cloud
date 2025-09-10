DOCKER_NETWORK = hadoop-network
VERSION_REPOSITORY_DOCKER = 1.0.0

################################### tools ###################################################################

remove-images:
	@if [ -n "$$(docker images -q)" ]; then \
    		docker rmi -f $$(docker images -q); \
    	else \
    		echo "Nenhuma imagem para remover."; \
    	fi

remove-all-services:
	docker service rm $(docker service ls -q)

stop-containers:
	docker rm -f $(docker ps -aq)

################################### create network ###########################################################
create-network:
	docker network create --driver overlay compass_network --attachable

################################### prepare mnt #############################################################
BASE_DIR := .

MNT_DIRECTORIES = \
	$(BASE_DIR)/mnt/mongodb \
	$(BASE_DIR)/mnt/mongodb_configData \
	$(BASE_DIR)/mnt/mongodb_init \
	$(BASE_DIR)/mnt/airflow \
	$(BASE_DIR)/mnt/airflow/dags \
	$(BASE_DIR)/mnt/airflow/logs \
	$(BASE_DIR)/mnt/airflow/plugins \
	$(BASE_DIR)/mnt/postgres-db-volume \
	$(BASE_DIR)/mnt/grafana_data

prepare-mnt:
	@for dir in $(MNT_DIRECTORIES); do \
		sudo mkdir -p $$dir && \
		sudo chown -R $(whoami):$(whoami) $$dir && \
		sudo chown -R airflow:airflow $$dir && \
		sudo chmod -R 755 $$dir; \
	done
	echo "Diretórios de montagem criados e permissões aplicadas com sucesso."

	sudo chmod 666 /var/run/docker.sock
	echo "Permissões 666 aplicadas ao /var/run/docker.sock"

init-mongo:
	sudo mkdir -p /mnt/mongodb_init && \
	echo "db = db.getSiblingDB('compass'); \
	db.createCollection('reviews-santander-way'); \
	db.createUser({ \
		user: 'app_user', \
		pwd: 'secure_password123', \
		roles: [{ role: 'readWrite', db: 'compass' }] \
	});" | sudo tee /mnt/mongodb_init/init-mongo.js > /dev/null

#################################### deployment environment production ########################################
deployment-mongodb-service:
	docker stack deploy -c layer_batch/DEPLOYMENT/deployment-database-mongodb-service.yaml deployment-mondodb

deployment-airflow-service:
	docker stack deploy -c layer_batch/DEPLOYMENT/deployment-airflow-service.yaml deployment-airflow

deployment-grafana-service:
	docker stack deploy -c layer_batch/DEPLOYMENT/deployment-observabilidade-service.yaml deployment-grafana
