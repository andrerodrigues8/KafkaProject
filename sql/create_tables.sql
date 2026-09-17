CREATE TABLE examples(
   example_id SERIAL PRIMARY KEY,
   example_name VARCHAR(40) NOT NULL
);

INSERT INTO examples (example_name) VALUES ('Test');