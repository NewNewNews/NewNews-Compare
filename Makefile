start:
	sudo docker compose up -d
	python server.py

stop:
	sudo docker compose down

client:
	python client.py