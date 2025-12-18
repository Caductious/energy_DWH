\c energy_data_vault;

CREATE TABLE gold.dim_recourse (
    recourse_id INTEGER PRIMARY KEY,
    recourse VARCHAR(50) NOT NULL
);

CREATE TABLE gold.dim_location (
    location_id INTEGER PRIMARY KEY,
    city VARCHAR(100),
    street VARCHAR(200),
    zipcode_from VARCHAR(10),
    zipcode_to VARCHAR(10),
    net_manager VARCHAR(200),
    purchase_area VARCHAR(200),
    type_of_connection VARCHAR(20),
    num_connections FLOAT
);

CREATE TABLE gold.fact_table (
    fact_id INTEGER PRIMARY KEY,
    annual_consume FLOAT,
    year INTEGER,
    perc_of_active_connections FLOAT,
    delivery_perc FLOAT,
    type_conn_perc FLOAT,
    location_id INTEGER,
    recourse_id INTEGER,
    
    CONSTRAINT fk_fact_location 
        FOREIGN KEY (location_id) 
        REFERENCES gold.dim_location(location_id),
    
    CONSTRAINT fk_fact_recourse 
        FOREIGN KEY (recourse_id) 
        REFERENCES gold.dim_recourse(recourse_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_location_id 
ON gold.fact_table(location_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_recourse_id
ON gold.fact_table(recourse_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fact_year 
ON gold.fact_table(year);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dim_location_city 
ON gold.dim_location(city);