rebuild:
	docker-compose down
	docker-compose up -d --build
	sleep 7s; psql "postgresql://admin:1234567@localhost:54321/go_service_2" -f rc_1.sql