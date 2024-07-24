import pandas as pd
import psycopg2
from time import time
import csv


class ReadTeryt:
    def __init__(self, dbname='teryt',
                 user='teryt_db',
                 password='teryt_db',
                 host='localhost',
                 port='5432',
                 terc='TERC_Adresowy_2024-07-22.csv',
                 simc='SIMC_Adresowy_2024-07-22.csv',
                 ulic='ULIC_Adresowy_2024-07-22.csv',
                 codes='kody_pocztowe.csv') -> None:
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        self.terc = terc
        self.simc = simc
        self.ulic = ulic
        self.codes = codes

    def _read_terc(self):
        df = pd.read_csv(self.terc,
                         sep=';', encoding='utf-8')
        df['WOJ'] = df['WOJ'].apply(lambda x: str(int(x)).zfill(2))
        df['POW'] = df['POW'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['GMI'] = df['GMI'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['NAZWA'] = df['NAZWA'].apply(lambda x: x.lower())

        wojewodztwa = df[(df['POW'] == '') & (df['GMI'] == '')
                         ][['WOJ', 'NAZWA', 'STAN_NA']]
        wojewodztwa = wojewodztwa.rename(
            columns={'WOJ': 'kod', 'NAZWA': 'nazwa', 'STAN_NA': 'stan_na'}
        )

        powiaty = df[(df['GMI'] == '') & (df['POW'] != '')
                     ][['WOJ', 'POW', 'NAZWA', 'STAN_NA']]
        powiaty = powiaty.rename(
            columns={'WOJ': 'kod_woj', 'POW': 'kod_pow',
                     'NAZWA': 'nazwa', 'STAN_NA': 'stan_na'}
        )

        gminy = df[df['GMI'] != ''][['WOJ', 'POW', 'GMI',
                                    'NAZWA', 'RODZ', 'NAZWA_DOD', 'STAN_NA']]
        gminy = gminy.rename(
            columns={
                'WOJ': 'kod_woj',
                'POW': 'kod_pow',
                'GMI': 'kod_gmi',
                'NAZWA': 'nazwa',
                'STAN_NA': 'stan_na',
                'RODZ': 'rodzaj',
                'NAZWA_DOD': 'typ_jednostki',
            }
        )
        return wojewodztwa, powiaty, gminy

    def _read_simc(filepath):
        df = pd.read_csv('SIMC_Adresowy_2024-07-22.csv',
                         sep=';', encoding='utf-8')

        df['WOJ'] = df['WOJ'].apply(lambda x: str(int(x)).zfill(2))
        df['POW'] = df['POW'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['GMI'] = df['GMI'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['RODZ_GMI'] = df['RODZ_GMI'].apply(lambda x: int(x))
        df['SYM'] = df['SYM'].apply(lambda x: str(int(x)).zfill(7))
        df['SYMPOD'] = df['SYMPOD'].apply(lambda x: str(int(x)).zfill(7))
        df['NAZWA'] = df['NAZWA'].apply(lambda x: x.lower())

        return df

    def _read_ulic(filepath):
        df = pd.read_csv('ULIC_Adresowy_2024-07-22.csv',
                         sep=';', encoding='utf-8')
        df['SYM'] = df['SYM'].apply(lambda x: str(int(x)).zfill(7))
        df['SYM_UL'] = df['SYM_UL'].apply(lambda x: str(int(x)).zfill(5))
        df['NAZWA_1'] = df['NAZWA_1'].apply(lambda x: x.lower())
        df['NAZWA_2'] = df['NAZWA_2'].apply(
            lambda x: x.lower() if pd.notna(x) else '')

        return df

    def _read_codes(self):
        codes = []
        with open(self.codes, newline='', encoding='utf-8-sig') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')
            for row in csvreader:
                codes.append({
                    'kod': row['Kod'],
                    'wojewodztwo': row['Województwo'].lower(),
                    'powiat': row['Powiat'].lower(),
                    'gmina': row['Gmina'].lower()
                })
        return codes

    def _clear_tables(self):
        print('Clearing tables')
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM ulice")
                print('Ulice cleared')
                cursor.execute("DELETE FROM gmina_kod_pocztowy")
                print('Gmina_kod_pocztowy cleared')
                cursor.execute("DELETE FROM kod_pocztowy")
                print('Kod_pocztowy cleared')
                cursor.execute("DELETE FROM miasta")
                print('Miasta cleared')
                cursor.execute("DELETE FROM gminy")
                print('Gminy cleared')
                cursor.execute("DELETE FROM powiaty")
                print('Powiaty cleared')
                cursor.execute("DELETE FROM wojewodztwa")
                print('Wojewodztwa cleared')
                self.conn.commit()
                print('Tables cleared')

    def _insert_woj(self, wojewodztwa):
        with self.conn:
            with self.conn.cursor() as cursor:
                for _, row in wojewodztwa.iterrows():
                    cursor.execute(
                        "INSERT INTO wojewodztwa (kod, nazwa, stan_na) VALUES (%s, %s, %s) RETURNING id",
                        (row['kod'], row['nazwa'], row['stan_na'])
                    )
                self.conn.commit()

    def _insert_pow(self, powiaty):
        with self.conn:
            with self.conn.cursor() as cursor:
                for _, row in powiaty.iterrows():
                    cursor.execute(
                        'SELECT id FROM wojewodztwa WHERE kod = %s', (row['kod_woj'],))
                    woj_id = cursor.fetchone()[0]
                    cursor.execute(
                        "INSERT INTO powiaty (kod, wojewodztwo_id, nazwa, stan_na) VALUES (%s, %s, %s, %s)",
                        (row['kod_pow'], woj_id, row['nazwa'], row['stan_na'])
                    )
                self.conn.commit()

    def _insert_gmina(self, gminy):
        with self.conn:
            with self.conn.cursor() as cursor:
                for _, row in gminy.iterrows():
                    cursor.execute("""
                        SELECT p.id
                        FROM powiaty p
                            JOIN wojewodztwa w ON w.id = p.wojewodztwo_id
                        WHERE w.kod = %s AND p.kod = %s;
                    """, (row['kod_woj'], row['kod_pow']))
                    powiat_id = cursor.fetchone()[0]
                    cursor.execute(
                        "INSERT INTO gminy (kod, powiat_id, nazwa, rodzaj, typ_jednostki, stan_na) VALUES (%s, %s, %s, %s, %s, %s)",
                        (row['kod_gmi'], powiat_id, row['nazwa'],
                         row['rodzaj'], row['typ_jednostki'], row['stan_na'])
                    )
                self.conn.commit()

    def _insert_miasta(self, miasta):
        with self.conn:
            with self.conn.cursor() as cursor:
                for _, row in miasta.iterrows():
                    cursor.execute("""
                        SELECT g.id
                        FROM gminy g
                            JOIN powiaty p ON p.id = g.powiat_id
                            JOIN wojewodztwa w ON w.id = p.wojewodztwo_id
                        WHERE w.kod = %s AND p.kod = %s AND g.kod = %s AND g.rodzaj = %s;
                    """, (row['WOJ'], row['POW'], row['GMI'], row['RODZ_GMI']))
                    gmina_id = cursor.fetchone()[0]
                    cursor.execute(
                        "INSERT INTO miasta (id, gmina_id, nazwa, miasto_podstawowe_id, stan_na) VALUES (%s, %s, %s, %s, %s)",
                        (row['SYM'], gmina_id, row['NAZWA'],
                         row['SYMPOD'], row['STAN_NA'])
                    )
                self.conn.commit()

    def _insert_ulice(self, ulice):
        with self.conn:
            with self.conn.cursor() as cursor:
                for _, row in ulice.iterrows():
                    cursor.execute("""
                        SELECT m.id
                        FROM miasta m
                        WHERE m.id = %s;
                    """, (row['SYM'],))
                    miasto_id = cursor.fetchone()[0]
                    if not row['NAZWA_2']:
                        cursor.execute("""
                            INSERT INTO ulice (nazwa_id, miasto_id, nazwa1, cecha, stan_na)
                            VALUES (%s, %s, %s, %s, %s)""", (row['SYM_UL'], miasto_id, row['NAZWA_1'], row['CECHA'], row['STAN_NA']))
                    else:
                        cursor.execute("""
                            INSERT INTO ulice (nazwa_id, miasto_id, nazwa1, nazwa2, cecha, stan_na)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (row['SYM_UL'], miasto_id, row['NAZWA_1'], row['NAZWA_2'], row['CECHA'], row['STAN_NA']))

                self.conn.commit()

    def _insert_codes(self, codes):
        with self.conn:
            with self.conn.cursor() as cursor:
                for code in codes:
                    # Sprawdzenie czy kod pocztowy już istnieje
                    cursor.execute(
                        "SELECT id FROM kod_pocztowy WHERE kod = %s", (code['kod'],))
                    result = cursor.fetchone()

                    # Jeśli kod pocztowy nie istnieje, dodajemy go
                    if result is None:
                        cursor.execute(
                            "INSERT INTO kod_pocztowy (kod) VALUES (%s) RETURNING id", (code['kod'],))
                        kod_id = cursor.fetchone()[0]
                    else:
                        kod_id = result[0]

                    try:
                        cursor.execute(
                            "SELECT id FROM wojewodztwa WHERE nazwa = %s", (code['wojewodztwo'],))
                        wojewodztwo_id = cursor.fetchone()[0]

                        cursor.execute(
                            "SELECT id FROM powiaty WHERE nazwa = %s AND wojewodztwo_id = %s", (code['powiat'], wojewodztwo_id))
                        powiat_id = cursor.fetchone()[0]

                        cursor.execute(
                            "SELECT id FROM gminy WHERE nazwa = %s AND powiat_id = %s", (code['gmina'], powiat_id))
                        gmina_id = cursor.fetchone()[0]
                    except TypeError as e:
                        continue

                    cursor.execute(
                        """
                        SELECT id FROM gmina_kod_pocztowy 
                        WHERE gmina_id = %s AND kod_id = %s
                        """,
                        (gmina_id, kod_id)
                    )
                    relation_exists = cursor.fetchone()

                    if relation_exists is None:
                        cursor.execute(
                            """
                            INSERT INTO gmina_kod_pocztowy (gmina_id, kod_id) 
                            VALUES (%s, %s)
                            """,
                            (gmina_id, kod_id)
                        )
                self.conn.commit()

    def _create_tables(self):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE wojewodztwa (
                        id SERIAL PRIMARY KEY,
                        kod VARCHAR(2) NOT NULL,
                        nazwa VARCHAR(100) NOT NULL,
                        stan_na DATE NOT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE powiaty (
                    id SERIAL PRIMARY KEY,
                    kod VARCHAR(2) NOT NULL,
                    wojewodztwo_id INT NOT NULL REFERENCES wojewodztwa(id) ON DELETE CASCADE,
                    nazwa VARCHAR(100) NOT NULL,
                    stan_na DATE NOT NULL
                );
                """)
                cursor.execute("""
                    CREATE TABLE gminy (
                        id SERIAL PRIMARY KEY,
                        kod VARCHAR(2) NOT NULL,
                        powiat_id INT NOT NULL REFERENCES powiaty(id) ON DELETE CASCADE,
                        nazwa VARCHAR(100) NOT NULL,
                        rodzaj INT NOT NULL,
                        typ_jednostki VARCHAR(50),
                        stan_na DATE NOT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE kod_pocztowy (
                        id SERIAL PRIMARY KEY,
                        kod VARCHAR(6) UNIQUE NOT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE gmina_kod_pocztowy (
                        id SERIAL PRIMARY KEY,
                        gmina_id INT NOT NULL REFERENCES gminy(id) ON DELETE CASCADE,
                        kod_id INT NOT NULL REFERENCES kod_pocztowy(id) ON DELETE CASCADE
                    );
                """)
                cursor.execute("""
                    CREATE TABLE miasta (
                        id VARCHAR(7) PRIMARY KEY,
                        gmina_id INT NOT NULL REFERENCES gminy(id) ON DELETE CASCADE,
                        nazwa VARCHAR(100) NOT NULL,
                        miasto_podstawowe_id VARCHAR(7) NOT NULL,
                        stan_na DATE NOT NULL
                    );
                """)
                cursor.execute("""
                    CREATE TABLE ulice (
                        id SERIAL PRIMARY KEY,
                        nazwa_id VARCHAR(5) NOT NULL,
                        miasto_id VARCHAR(7) NOT NULL REFERENCES miasta(id) ON DELETE CASCADE,
                        nazwa1 VARCHAR(100) NOT NULL,
                        nazwa2 VARCHAR(100),
                        cecha VARCHAR(5),
                        stan_na DATE NOT NULL
                    );
                """)
                self.conn.commit()

    def populate_db(self):
        print('Connected to database')
        self._clear_tables()
        print('Tables cleared')
        wojewodztwa, powiaty, gminy = self._read_terc()
        print('TERC read')
        miasta = self._read_simc()
        print('SIMC read')
        ulice = self._read_ulic()
        print('ULIC read')
        codes = self._read_codes()
        print('Codes read')
        self._insert_woj(wojewodztwa)
        print('Województwa inserted')
        self._insert_pow(powiaty)
        print('Powiaty inserted')
        self._insert_gmina(gminy)
        print('Gminy inserted')
        self._insert_miasta(miasta)
        print('Miasta inserted')
        self._insert_ulice(ulice)
        print('Ulice inserted')
        self._insert_codes(codes)
        print('Codes inserted')
        print('Connection closed')


if __name__ == '__main__':
    ReadTeryt().populate_db()
