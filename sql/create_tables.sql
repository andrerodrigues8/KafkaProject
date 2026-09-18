CREATE TABLE route_suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE route_capacities (
    id SERIAL PRIMARY KEY,
    capacity INTEGER NOT NULL
);

INSERT INTO route_suppliers (name) VALUES
    ('CityBus'),
    ('MetroTrans'),
    ('FastTaxi'),
    ('UrbanRail'),
    ('ScooterGo');

INSERT INTO route_capacities (capacity) VALUES
    (1),
    (10),
    (20),
    (50),
    (100),
    (200);