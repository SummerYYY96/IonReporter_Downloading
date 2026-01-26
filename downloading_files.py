#!/usr/bin/env python3
"""
This script downloads vcfs and BAMs from Ion Reporter Server given a sample ID

Run:
  python downloading_files.py --config config.conf --sample SAMPLE123 --variants
  python downloading_files.py --config config.conf --sample SAMPLE123 --bams
"""

import argparse
import configparser
import glob
import os
import shutil
import subprocess
import zipfile

import requests


def load_config_ini(config_path: str):
    cp = configparser.ConfigParser()
    read_ok = cp.read(config_path)
    if not read_ok:
        raise FileNotFoundError(f"Could not read config file: {config_path}")

    d = cp["DEFAULT"]

    host = d.get("HOST", "").strip()
    token = d.get("TOKEN", "").strip()
    uid = d.get("UID", "").strip().strip('"').strip("'")

    bam_dir = d.get("BAM_DOWNLOADS_DIR", "").strip()

    # Optional for variants workflow (support both VAR_HOME and VAR_DIR)
    var_home = (d.get("VAR_DIR", "").strip() or None)

    # Optional URL rewrite
    rewrite_from = d.get("REWRITE_FROM", "").strip() or None
    rewrite_to = d.get("REWRITE_TO", "").strip() or None

    if not host:
        raise ValueError("Missing HOST in [DEFAULT]")
    if not token:
        raise ValueError("Missing TOKEN in [DEFAULT]")
    if not bam_dir:
        raise ValueError("Missing BAM_DOWNLOADS_DIR in [DEFAULT]")

    return {
        "HOST": host,
        "TOKEN": token,
        "UID": uid,
        "BAM_DIR": bam_dir,
        "VAR_HOME": var_home,
        "REWRITE_FROM": rewrite_from,
        "REWRITE_TO": rewrite_to,
    }


# -----------------------------
# Minimal-change class
# -----------------------------
class IonReporterDownloader:
    def __init__(self, config_path: str):
        cfg = load_config_ini(config_path)

        self.HOST = cfg["HOST"]
        self.TOKEN = cfg["TOKEN"]
        self.UID = cfg["UID"]

        # Paths: BAM is required by config; the others are only required for variants
        self.BAM_DIR = cfg["BAM_DIR"]

        self.VAR_HOME = cfg.get("VAR_HOME")

        self.REWRITE_FROM = cfg.get("REWRITE_FROM")
        self.REWRITE_TO = cfg.get("REWRITE_TO")

        os.makedirs(self.BAM_DIR, exist_ok=True)

        # Only create these if provided (variants mode)
        if self.VAR_HOME:
            os.makedirs(self.VAR_HOME, exist_ok=True)

    def _rewrite_url_if_needed(self, url: str):
        if self.REWRITE_FROM and self.REWRITE_TO:
            return url.replace(self.REWRITE_FROM, self.REWRITE_TO, 1)
        return url

    # -------------------------
    # Your original functions
    # -------------------------
    def get_download_link(self, sample):
        """
        Return a tuple (download_link, name) or None if not found.
        Iterates v1, v2, ... and returns the last valid one.
        """
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": self.TOKEN,
            }
            i = 1
            last_ok = None

            while True:
                params = {"name": f"{sample}_v{i}", "format": "json"}
                r = requests.get(
                    f"https://{self.HOST}/api/v1/getvcf",
                    headers=headers,
                    params=params,
                    verify=False,  
                    timeout=180,
                )
                print(r.status_code, r.text)
                if r.status_code == 200:
                    last_ok = r
                    i += 1
                else:
                    break

            if not last_ok:
                return None

            j = last_ok.json()

            # Pull out fields depending on structure
            if isinstance(j, list) and j:
                data_links = j[0].get("data_links")
                name_field = j[0].get("name")
            elif isinstance(j, dict):
                data_links = j.get("data_links")
                name_field = j.get("name")
            else:
                data_links = None
                name_field = None

            # Normalize data_links to a single string
            if isinstance(data_links, list):
                data_links = data_links[0] if data_links else None

            if data_links:
                return str(data_links), name_field
            else:
                return None
        except Exception:
            return None

    def download_zip(self, sample):
        """
        download the zip from the returned link, unzip locally.
        """
        try:
            print("downloading zip")

            # Minimal safety tweak: your original would crash if get_download_link() is None
            link = self.get_download_link(sample)
            download_link = link[0] if link else None

            if not download_link:
                return None

            download_link = self._rewrite_url_if_needed(download_link)
            print("printing:", download_link)

            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": self.TOKEN,
            }

            r = requests.get(
                download_link,
                headers=headers,
                timeout=120,
                allow_redirects=False,
                stream=True,
                verify=False,  
            )

            if not self.VAR_HOME:
                raise ValueError("VAR_HOME not set in config.ini; required for downloading variant zip.")

            os.makedirs(self.VAR_HOME, exist_ok=True)
            output_zip = os.path.join(self.VAR_HOME, "temp.zip")

            with open(output_zip, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:  # keep-alive chunks can be empty
                        f.write(chunk)
                print(f"Saved: {output_zip}")
            return output_zip

        except Exception as e:
            import traceback
            print("ERROR:", type(e).__name__, str(e), flush=True)
            traceback.print_exc()
            return None

    def get_tsv_file(self, sample):
        """
        From the unzipped directory, locate and return the three files (tsv, vcf, oncomine tsv).
        or (None, None, None) if not found
        """
        if not self.VAR_HOME:
            raise ValueError(
                "Missing paths for variants mode. Please set VAR_HOME")

        temp_dir = os.path.join(self.VAR_HOME, "temp")
        # Always start fresh
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)

        temp_zip = self.download_zip(sample)
        print(f"temp zip is {temp_zip}")
        if not temp_zip:
            return None, None, None

        # Step 1: unzip temp.zip into temp_dir
        with zipfile.ZipFile(temp_zip, "r") as z:
            z.extractall(temp_dir)

        # Step 2: find nested zip file inside temp_dir
        nested_zip = None
        for root, _, files in os.walk(temp_dir):
            for f in files:
                if f.endswith(".zip"):
                    nested_zip = os.path.join(root, f)
                    break
        if not nested_zip:
            raise FileNotFoundError("No nested zip found in temp.zip")

        # Step 3: unzip the nested zip
        nested_dir = os.path.join(temp_dir, "nested")
        os.makedirs(nested_dir, exist_ok=True)
        print(f"Made the nexted directory {nested_dir}")
        with zipfile.ZipFile(nested_zip, "r") as z:
            z.extractall(nested_dir)

        # Step 4: find the *subdirectory* under "Variants"
        target_subdir = None
        for root, dirs, _ in os.walk(nested_dir):
            if os.path.basename(root) == "Variants":
                if dirs:
                    # assume only one target directory inside Variants
                    target_subdir = os.path.join(root, dirs[0])
                    print(target_subdir)
                break
        if not target_subdir:
            raise FileNotFoundError("No subdirectory under Variants found")

        # Step 5: copy that subdir to downloads_dir / VAR_HOME
        dest_path = os.path.join(self.VAR_HOME, os.path.basename(target_subdir))
        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)
        shutil.copytree(target_subdir, dest_path)

        # Step 6: clean up
        try:
            os.remove(temp_zip)
        except OSError:
            pass
        shutil.rmtree(temp_dir, ignore_errors=True)

        print(f"Saved directory {target_subdir} to {self.VAR_HOME}")

        sample_pair = os.path.basename(target_subdir)
        file_path = os.path.join(self.VAR_HOME, sample_pair)

        return (
            glob.glob(os.path.join(file_path, "%s*_Filtered_*.vcf" % sample_pair))[0]
        )

# adding BAM downloading 
    def download_bam_file(self, url: str, sample: str):
        """
        Download a BAM file from the given inputBam URL and save it as {sample_name}.bam.
        """
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "auth": self.TOKEN,
            "Connection": "close"
        }
        # folder_name = f"{run_id}-{chip_number}"
        # run_path = os.path.join(self.BAM_DIR, folder_name)
        # os.makedirs(run_path, exist_ok=True)

        raw_bam_name = url.rsplit("/", 1)[-1]

        if not raw_bam_name.endswith(".bam"):
            raise ValueError(f"URL does not point to a .bam file: {raw_bam_name}")
        
        if not raw_bam_name.startswith("IonXpress"):
            raise ValueError(f"BAM ID must start with 'IonXpress': {raw_bam_name}")

        if raw_bam_name.endswith("_rawlib.bam"):
            raw_bam_id = raw_bam_name[:-len("_rawlib.bam")]
        else:
            raw_bam_id = raw_bam_name[:-len(".bam")]
        out_path = os.path.join(self.BAM_DIR, f"{raw_bam_id}_{sample}.bam")
        
        try:
            with requests.get(url, headers=headers, stream=True, verify=False, timeout=600) as r:
                r.raise_for_status()
                print(f"Downloading {sample} BAM files")
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return out_path
        except requests.RequestException as e:
            print(f"Failed to download {sample}: {e}")
            return None

    def index_bam(self, bam_path):
        """Generate a .bai index for IGV viewing."""
        bai_path = bam_path + ".bai"

        subprocess.run(["samtools", "index", bam_path, bai_path], check=True)
        return bai_path

    def fetch_and_download_bams(self, sample: str):
        """
        Fetch inputBam links for an analysis and download the BAM files.
        
        Returns a dict mapping sampleName -> bam_path.
        """
        link = self.get_download_link(sample)
        analysis = link[1] if link else None
        if not analysis:
            print(f"No analysis found for sample: {sample}")
            return {}
        results = {}
        url = f"https://{self.HOST}/api/v1/getAssociatedBamfiles"
        params = {"name": analysis, "type": "analysis"}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.TOKEN}
        
        try:
            resp = requests.get(url, headers=headers, params=params, verify=False, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            
            for item in data:
                for sample in item.get("sampleDetails", []):
                    if sample.get("sampleRole") != "dna":
                        continue

                    sample_name = sample.get("sampleName")
                    input_bams = sample.get("inputBam", [])
                    if input_bams:
                        bam_url = input_bams[0]  # usually one inputBam
                        bam_url = self._rewrite_url_if_needed(bam_url)
                        downloaded_bam = self.download_bam_file(bam_url, sample_name)
                        # generate bai
                        if downloaded_bam:
                            print(f"Indexing BAM for {sample_name}")
                            downloaded_bai = self.index_bam(downloaded_bam)
                        else:
                            downloaded_bai = None

                        results[sample_name] = {
                            "bam": downloaded_bam,
                            "bai": downloaded_bai
                        } # return this simply for debugging in future 
            return results
        
        except requests.RequestException as e:
            print(f"Error fetching input BAMs: {e}")
            return {}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--sample", required=True)
    parser.add_argument("--variants", action="store_true")
    parser.add_argument("--bams", action="store_true")
    args = parser.parse_args()

    d = IonReporterDownloader(args.config)

    if args.variants:
        filtered_vcf= d.get_tsv_file(args.sample)

    if args.bams:
        print(d.fetch_and_download_bams(args.sample))

if __name__ == "__main__":
    main()
