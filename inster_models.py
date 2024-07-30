import pandas as pd
import csv
from datetime import datetime
from myapp.models import Province, County, Parish, PostalCode, ParishPostalCode, City, Street


class ReadTeryt:
    def __init__(self,
                 terc='TERC_Adresowy_2024-07-22.csv',
                 simc='SIMC_Adresowy_2024-07-22.csv',
                 ulic='ULIC_Adresowy_2024-07-22.csv',
                 codes='kody_pocztowe.csv') -> None:
        self.terc = terc
        self.simc = simc
        self.ulic = ulic
        self.codes = codes

    def _read_terc(self):
        df = pd.read_csv(self.terc, sep=';', encoding='utf-8')
        df['WOJ'] = df['WOJ'].apply(lambda x: str(int(x)).zfill(2))
        df['POW'] = df['POW'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['GMI'] = df['GMI'].apply(lambda x: str(
            int(x)).zfill(2) if pd.notna(x) else '')
        df['NAZWA'] = df['NAZWA'].apply(lambda x: x.lower())

        wojewodztwa = df[(df['POW'] == '') & (df['GMI'] == '')
                         ][['WOJ', 'NAZWA', 'STAN_NA']]
        wojewodztwa = wojewodztwa.rename(
            columns={'WOJ': 'code', 'NAZWA': 'name', 'STAN_NA': 'valid_date'})

        powiaty = df[(df['GMI'] == '') & (df['POW'] != '')
                     ][['WOJ', 'POW', 'NAZWA', 'STAN_NA']]
        powiaty = powiaty.rename(columns={
                                 'WOJ': 'code_province', 'POW': 'code_county', 'NAZWA': 'name', 'STAN_NA': 'valid_date'})

        gminy = df[df['GMI'] != ''][['WOJ', 'POW', 'GMI',
                                     'NAZWA', 'RODZ', 'NAZWA_DOD', 'STAN_NA']]
        gminy = gminy.rename(columns={'WOJ': 'code_province', 'POW': 'code_county', 'GMI': 'code_parish',
                             'NAZWA': 'name', 'STAN_NA': 'valid_date', 'RODZ': 'type', 'NAZWA_DOD': 'unit_type'})

        return wojewodztwa, powiaty, gminy

    def _read_simc(self):
        df = pd.read_csv(self.simc, sep=';', encoding='utf-8')
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

    def _read_ulic(self):
        df = pd.read_csv(self.ulic, sep=';', encoding='utf-8')
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
                    'code': row['Kod'],
                    'province': row['Województwo'].lower(),
                    'county': row['Powiat'].lower(),
                    'parish': row['Gmina'].lower()
                })
        return codes

    def _clear_tables(self):
        print('Clearing tables')
        Parish.objects.all().delete()
        ParishPostalCode.objects.all().delete()
        PostalCode.objects.all().delete()
        City.objects.all().delete()
        Street.objects.all().delete()
        County.objects.all().delete()
        Province.objects.all().delete()
        print('Tables cleared')

    def _insert_provinces(self, wojewodztwa):
        for _, row in wojewodztwa.iterrows():
            Province.objects.get_or_create(
                code=row['code'],
                defaults={'name': row['name'], 'valid_date': row['valid_date']}
            )

    def _insert_counties(self, powiaty):
        for _, row in powiaty.iterrows():
            province = Province.objects.get(code=row['code_province'])
            County.objects.get_or_create(
                code=row['code_county'],
                province=province,
                defaults={'name': row['name'], 'valid_date': row['valid_date']}
            )

    def _insert_parishes(self, gminy):
        for _, row in gminy.iterrows():
            county = County.objects.get(
                code=row['code_county'], province__code=row['code_province'])
            Parish.objects.get_or_create(
                code=row['code_parish'],
                county=county,
                defaults={
                    'name': row['name'],
                    'parish_type': row['type'],
                    'unit_type': row['unit_type'],
                    'valid_date': row['valid_date']
                }
            )

    def _insert_cities(self, miasta):
        for _, row in miasta.iterrows():
            parish = Parish.objects.get(code=row['GMI'])
            City.objects.get_or_create(
                id=row['SYM'],
                parish=parish,
                defaults={
                    'name': row['NAZWA'], 'primary_city_id': row['SYMPOD'], 'valid_date': row['STAN_NA']}
            )

    def _insert_streets(self, ulice):
        for _, row in ulice.iterrows():
            city = City.objects.get(id=row['SYM'])
            Street.objects.get_or_create(
                name_id=row['SYM_UL'],
                city=city,
                defaults={
                    'name1': row['NAZWA_1'],
                    'name2': row['NAZWA_2'],
                    'street_type': row['CECHA'],
                    'valid_date': row['STAN_NA']
                }
            )

    def _insert_codes(self, codes):
        for code in codes:
            postal_code, created = PostalCode.objects.get_or_create(
                code=code['code'])
            if created:
                print(f"Inserted PostalCode: {postal_code.code}")

            try:
                parish = Parish.objects.get(name=code['parish'])
                ParishPostalCode.objects.get_or_create(
                    parish=parish,
                    postal_code=postal_code
                )
            except Parish.DoesNotExist:
                print(f"Parish not found for code: {code['parish']}")

    def populate_db(self):
        print('Clearing tables')
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
        self._insert_provinces(wojewodztwa)
        print('Provinces inserted')
        self._insert_counties(powiaty)
        print('Counties inserted')
        self._insert_parishes(gminy)
        print('Parishes inserted')
        self._insert_cities(miasta)
        print('Cities inserted')
        self._insert_streets(ulice)
        print('Streets inserted')
        self._insert_codes(codes)
        print('Codes inserted')
        print('Database population complete')


if __name__ == '__main__':
    ReadTeryt().populate_db()
