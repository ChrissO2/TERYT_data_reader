CREATE TABLE wojewodztwa (
    id SERIAL PRIMARY KEY,
    kod VARCHAR(2) NOT NULL,
    nazwa VARCHAR(100) NOT NULL,
    stan_na DATE NOT NULL
);

CREATE TABLE powiaty (
    id SERIAL PRIMARY KEY,
    kod VARCHAR(2) NOT NULL,
    wojewodztwo_id INT NOT NULL REFERENCES wojewodztwa(id) ON DELETE CASCADE,
    nazwa VARCHAR(100) NOT NULL,
    stan_na DATE NOT NULL
);

CREATE TABLE gminy (
    id SERIAL PRIMARY KEY,
    kod VARCHAR(2) NOT NULL,
    powiat_id INT NOT NULL REFERENCES powiaty(id) ON DELETE CASCADE,
    nazwa VARCHAR(100) NOT NULL,
    rodzaj INT NOT NULL,
    typ_jednostki VARCHAR(50),
    stan_na DATE NOT NULL
);

CREATE TABLE kod_pocztowy (
    id SERIAL PRIMARY KEY,
    kod VARCHAR(6) UNIQUE NOT NULL
);

CREATE TABLE gmina_kod_pocztowy (
    id SERIAL PRIMARY KEY,
    gmina_id INT NOT NULL REFERENCES gminy(id) ON DELETE CASCADE,
    kod_id INT NOT NULL REFERENCES kod_pocztowy(id) ON DELETE CASCADE
);

CREATE TABLE miasta (
    id VARCHAR(7) PRIMARY KEY,
    gmina_id INT NOT NULL REFERENCES gminy(id) ON DELETE CASCADE,
    nazwa VARCHAR(100) NOT NULL,
    miasto_podstawowe_id VARCHAR(7) NOT NULL,
    stan_na DATE NOT NULL
);

CREATE TABLE ulice (
    id SERIAL PRIMARY KEY,
    nazwa_id VARCHAR(5) NOT NULL,
    miasto_id VARCHAR(7) NOT NULL REFERENCES miasta(id) ON DELETE CASCADE,
    nazwa1 VARCHAR(100) NOT NULL,
    nazwa2 VARCHAR(100),
    cecha VARCHAR(5),
    stan_na DATE NOT NULL
);