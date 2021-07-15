`CREATE TABLE users (
	first_name STRING(MAX),
	last_name  STRING(MAX),
	country    STRING(MAX),
	email      STRING(MAX),
	age        FLOAT64,
) PRIMARY KEY (first_name, email)`;
`CREATE TABLE products (
	category    STRING(MAX),
  	description STRING(MAX),
  	name        STRING(MAX),
  	price       FLOAT64,
) PRIMARY KEY (name, category)`;
`CREATE TABLE carts (
	id          STRING(MAX),
	total_value FLOAT64,
  	products    ARRAY<STRING>,
) PRIMARY KEY (id)`