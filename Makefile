ifneq (,$(wildcard ./.env))
    include .env
    export
endif

build:
	@echo "Building..."
	@go build 
	

up:
	@echo "Starting..."
	go run main.go up -d $(DATAMIGRATIONS_DIR) -c $(DB_URL) -p $(MIGRATIONS_DIR)
	
down:
	@echo "Starting..."
	go run main.go down -d $(DATAMIGRATIONS_DIR) -c $(DB_URL) -p $(MIGRATIONS_DIR)
	
down-up:
	@echo "Starting..."
	go run main.go down -d $(DATAMIGRATIONS_DIR) -c $(DB_URL) -p $(MIGRATIONS_DIR)
	go run main.go up -d $(DATAMIGRATIONS_DIR) -c $(DB_URL) -p $(MIGRATIONS_DIR)	
	
create:
	@echo "Starting..."
	go run main.go create -d $(DATAMIGRATIONS_DIR) -c $(DB_URL) -p $(MIGRATIONS_DIR) -version $(@version)