CREATE TABLE breed (
    breed_pk INT IDENTITY,
    name VARCHAR(12),
    --PRIMARY KEY rabbit_pk
)

CREATE TABLE rabbit (
    rabbit_pk INT IDENTITY,
    name VARCHAR(12),
    breed_fk INT,
    --PRIMARY KEY rabbit_pk
    FOREIGN KEY (breed_fk) REFERENCES breed (breed_pk)
)

CREATE TABLE human (
    human_pk INT IDENTITY,
    name VARCHAR(12)
    --PRIMARY KEY owner_pk
)

CREATE TABLE rabbit_human (
    rabbit_human_pk INT IDENTITY,
    rabbit_fk INT,
    human_fk INT,
    FOREIGN KEY (rabbit_fk) REFERENCES rabbit (rabbit_pk),
    FOREIGN KEY (human_fk) REFERENCES human (human_pk),
)
