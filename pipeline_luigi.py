import os
import luigi
import requests
import tarfile
import shutil
import pandas as pd
import io

class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter(default="GSE68849")
    output_dir = luigi.Parameter(default="data/raw")

    def output(self):
        return luigi.LocalTarget(f"{self.output_dir}/{self.dataset_name}_RAW.tar")

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={self.dataset_name}"
        # Parse download link (simplified)
        response = requests.get(url)
        download_link = f"https://ftp.ncbi.nlm.nih.gov/geo/series/GSE68nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        response = requests.get(download_link, stream=True)
        with open(self.output().path, 'wb') as f:
            shutil.copyfileobj(response.raw, f)

class ExtractFiles(luigi.Task):
    dataset_name = luigi.Parameter(default="GSE68849")
    raw_dir = luigi.Parameter(default="data/raw")
    output_dir = luigi.Parameter(default="data/extracted")

    def requires(self):
        return DownloadDataset(dataset_name=self.dataset_name, output_dir=self.raw_dir)

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        with tarfile.open(self.input().path, 'r') as tar:
            tar.extractall(self.output_dir)

class ProcessFiles(luigi.Task):
    dataset_name = luigi.Parameter(default="GSE68849")
    extracted_dir = luigi.Parameter(default="data/extracted")
    processed_dir = luigi.Parameter(default="data/processed")

    def requires(self):
        return ExtractFiles(dataset_name=self.dataset_name, raw_dir=self.extracted_dir)

    def output(self):
        return luigi.LocalTarget(self.processed_dir)

    def run(self):
        os.makedirs(self.processed_dir, exist_ok=True)
        for root, _, files in os.walk(self.input().path):
            for file in files:
                if file.endswith(".gz"):
                    file_path = os.path.join(root, file)
                    file_output_dir = os.path.join(self.processed_dir, file.split(".")[0])
                    os.makedirs(file_output_dir, exist_ok=True)
                    shutil.unpack_archive(file_path, file_output_dir)
                    self.process_text_files(file_output_dir)

    def process_text_files(self, dir_path):
        for file in os.listdir(dir_path):
            if file.endswith(".txt"):
                file_path = os.path.join(dir_path, file)
                dfs = {}
                with open(file_path, 'r') as f:
                    write_key = None
                    fio = io.StringIO()
                    for line in f:
                        if line.startswith('['):
                            if write_key:
                                fio.seek(0)
                                dfs[write_key] = pd.read_csv(fio, sep='\t', header='infer')
                            fio = io.StringIO()
                            write_key = line.strip('[]\n')
                            continue
                        if write_key:
                            fio.write(line)
                    fio.seek(0)
                    dfs[write_key] = pd.read_csv(fio, sep='\t')

                for key, df in dfs.items():
                    output_path = os.path.join(dir_path, f"{key}.tsv")
                    df.to_csv(output_path, sep='\t', index=False)

class FilterProbes(luigi.Task):
    dataset_name = luigi.Parameter(default="GSE68849")
    processed_dir = luigi.Parameter(default="data/processed")

    def requires(self):
        return ProcessFiles(dataset_name=self.dataset_name)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.processed_dir, "filtered"))

    def run(self):
        filtered_dir = self.output().path
        os.makedirs(filtered_dir, exist_ok=True)
        for root, _, files in os.walk(self.input().path):
            for file in files:
                if "Probes" in file and file.endswith(".tsv"):
                    df = pd.read_csv(os.path.join(root, file), sep='\t')
                    drop_cols = ["Definition", "Ontology_Component", "Ontology_Process",
                                 "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"]
                    filtered_df = df.drop(columns=[col for col in drop_cols if col in df.columns])
                    filtered_df.to_csv(os.path.join(filtered_dir, file), sep='\t', index=False)

if __name__ == "__main__":
    luigi.run()