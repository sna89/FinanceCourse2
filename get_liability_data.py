import requests
import zipfile
from multiprocessing import Pool

URL = 'https://www.sec.gov/files/dera/data/financial-statement-and-notes-data-sets/{year}q{quarter}_notes.zip'
ZIP_NAME = '{year}q{quarter}_notes.zip'

urls = [URL.format(year=year, quarter=quarter) for year in range(2009, 2020) for quarter in range(1, 5)]
zip_names = [ZIP_NAME.format(year=year, quarter=quarter) for year in range(2009, 2020) for quarter in range(1, 5)]


class Zipper:
    def __call__(self, zipped_url_zip_name):
        self.url = zipped_url_zip_name[0]
        self.zip_name = zipped_url_zip_name[1]
        self.get_zip(self.url, self.zip_name)

    @staticmethod
    def get_zip(url, zip_name):
        response = requests.get(url, stream=True)
        with open(zip_name, 'wb') as zfile:
            zfile.write(response.content)


class Extractor:
    def __call__(self, zip_name):
        self.extract_zip(zip_name)

    @staticmethod
    def extract_zip(zip_name):
        folder_name = zip_name.replace('.zip', '')
        with zipfile.ZipFile(zip_name, 'r') as zip_ref:
            zip_ref.extractall(folder_name)


def main():
    p = Pool(4)
    p.map(Zipper(), zip(urls, zip_names))
    p.map(Extractor(), zip_names)


main()




