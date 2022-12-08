from bs4 import BeautifulSoup
import boto3
import datetime
import json
import pandas as pd
import requests

class hls_s2_reconcile:

    def __init__(self):
        with open("reconcile_params.json", "r") as f:
            self.params = json.load(f)
        self.hls_tilelist = requests.get(self.params["tile_list_url"]).text.split("\n")
        self.missing_s2_files = {}
        self.modified_s2_files = {}
        self.archive_bucket = self.params["archive_bucket"]
        self.s3 = boto3.client("s3")
        self.get_copernicus_inventory_files()
        for date in self.missing_s2_files:
            print(f"{date}: {len(self.missing_s2_files[date])}")
        self.write_missing_filelist()
        self.write_modified_filelist()

    def get_archive_key(self, filename):
        file_comp = filename.split("_")
        date = datetime.datetime.strptime(file_comp[2],"%Y%m%dT%H%M%S")
        utm_info = file_comp[5]
        utm_zone = utm_info[1:3]
        lat_band = utm_info[3]
        square = utm_info[4:]
        new_key = f"{utm_zone}/{lat_band}/{square}/{date:%Y/%-m/%-d}/{filename}.zip"
        return new_key

    def get_copernicus_inventory_files(self):
        self.start_date = datetime.datetime.strptime(self.params["start_date"],"%Y-%m-%d")
        self.end_date = datetime.datetime.strptime(self.params["end_date"],"%Y-%m-%d")
        copernicus_url = self.params["copernicus_url"]
        for satellite in self.params["satellites"]:
            url_date = self.start_date
            while url_date <= self.end_date:
                data_url = f"{copernicus_url}/{satellite}/{url_date:%Y/%m}/"
                request = requests.get(data_url)
                print(f"Started download of data for {url_date:%m-%Y} from {satellite} at {datetime.datetime.now():%Y-%m-%dT%H:%M:%S}")
                self.process_copernicus_csvs(request, url_date)
                print(f"Finished download of data for {url_date:%m-%Y} from {satellite} at {datetime.datetime.now():%Y-%m-%dT%H:%M:%S}")
                url_date += datetime.timedelta(days=31)
                url_date = url_date.replace(day=1)

    def process_copernicus_csvs(self, request, url_date):
        total_size = 0
        count = 0
        soup = BeautifulSoup(request.text,"lxml")
        for link in soup.find_all("a"):
            if link.text in ["Name", "Last modified", "Size", "Description", "Parent Directory"]:
                pass
            else:
                csv_url = request.url + link["href"]
                yyyymmdd = csv_url.split("_")[1]
                date = datetime.datetime.strptime(yyyymmdd, "%Y%m%d")
                print(date)
                key = f"{date:%m-%d-%Y}"
                if date < self.start_date or date > self.end_date:
                    pass
                else:
                    if self.missing_s2_files.get(key) is None:
                        self.missing_s2_files[key] = []
                    if self.modified_s2_files.get(key) is None:
                        self.modified_s2_files[key] = []
                    df = pd.read_csv(csv_url, delimiter=",", header=0)
                    l1c = df[df["Name"].str.contains("MSIL1C")].copy(deep=False)
                    l1c["IngestionDate"] = pd.to_datetime(l1c["IngestionDate"], utc=True)
                    for i in range(len(l1c)):
                        fname = l1c["Name"].iloc[i]
                        archive_key = self.get_archive_key(fname)
                        size = l1c["ContentLength"].iloc[i]
                        last_modified = l1c["IngestionDate"].iloc[i]
                        obj = self.s3.list_objects_v2(Bucket=self.archive_bucket, Prefix=archive_key)
                        nkeys = obj["KeyCount"]
                        s3_size = obj.get("Contents",[{"Size":0}])[0]["Size"]
                        s3_modified = obj.get("Contents",[{"LastModified":datetime.datetime(1970,1,1,0,0,0)}])[0]["LastModified"]
                        if fname.split("_")[5][1:] in self.hls_tilelist and (nkeys == 0 or last_modified > s3_modified or abs(s3_size - size) > 5):
                            print(f"{fname} - keys: {nkeys}, s3 modified: {s3_modified}, ESA modified: {last_modified}")
                            count += 1
                            total_size += size/(1024*1024*1024*1024) # B to TB
                            self.missing_s2_files[key].append(fname)
                        #elif s3_size > size:
                        #    summary = {"filename": fname, "size": {"downloaded": s3_size, "inventory": int(size)}}
                        #    self.modified_s2_files[key].append(summary)
        sensor = fname.split("_")[0]
        print(f"Sensor: {sensor} Month-Year: {url_date:%m-%Y}, Count:{count}, Size: {total_size}")

    def write_missing_filelist(self):
        with open(f"missing_scenes_{self.start_date:%Y%m%d}_{self.end_date:%Y%m%d}.json", "w") as f:
            json.dump(self.missing_s2_files,f)

    def write_modified_filelist(self):
        with open(f"modified_scenes_{self.start_date:%Y%m%d}_{self.end_date:%Y%m%d}.json", "w") as f:
            json.dump(self.modified_s2_files,f)
if __name__ == "__main__":
    hls_s2_reconcile()
