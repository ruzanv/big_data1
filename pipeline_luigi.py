import luigi
import os
import tarfile
import gzip
import requests
import pandas as pd
import io
from pathlib import Path

class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default="data")

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.dataset_name}_RAW.tar"))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)

        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        print(f"Load Files: {url}")

        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}")

        with open(self.output().path, "wb") as f:
            f.write(response.content)
        print(f"Success: {self.output().path}")


class ExtractAndProcessFiles(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return DownloadDataset(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, self.dataset_name, "processed_done.txt"))

    def run(self):
        main_dir = Path(self.output_dir) / self.dataset_name
        os.makedirs(main_dir, exist_ok=True)

        tar_path = self.input().path
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(main_dir)

        for gz_file in main_dir.glob("*.gz"):
            extracted_file_path = gz_file.with_suffix("")
            extracted_dir = main_dir / extracted_file_path.stem
            os.makedirs(extracted_dir, exist_ok=True)

            print(f"Processing: {gz_file}")
            with gzip.open(gz_file, "rb") as f_in, open(extracted_dir / extracted_file_path.name, "wb") as f_out:
                f_out.write(f_in.read())

            print(f"Extracted {gz_file} to {extracted_dir}")
            gz_file.unlink()

            text_file = extracted_dir / extracted_file_path.name
            self.process_text_file(text_file)

        with self.output().open("w") as f:
            f.write("Processing completed")

    def process_text_file(self, file_path):
        dfs = {}
        with open(file_path, "r", encoding="utf-8") as f:
            write_key = None
            fio = io.StringIO()
            for line in f:
                if line.startswith("["):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == "Heading" else "infer"
                        dfs[write_key] = pd.read_csv(fio, sep="\t", header=header)

                    fio = io.StringIO()
                    write_key = line.strip("[]\n")
                    continue
                if write_key:
                    fio.write(line)

            fio.seek(0)
            if write_key:
                dfs[write_key] = pd.read_csv(fio, sep="\t")

        for key, df in dfs.items():
            output_file = file_path.parent / f"{key}.tsv"
            df.to_csv(output_file, sep="\t", index=False)

class TrimProbesTable(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return ExtractAndProcessFiles(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        main_dir = Path(self.output_dir) / self.dataset_name
        return [
            luigi.LocalTarget(probes_file.with_name(f"{probes_file.stem}_trimmed.tsv"))
            for probes_file in main_dir.rglob("Probes.tsv")
        ]

    def run(self):
        main_dir = Path(self.output_dir) / self.dataset_name

        probes_files = list(main_dir.rglob("Probes.tsv"))
        if not probes_files:
            raise FileNotFoundError(f"Files Probes.tsv not found")
        columns_to_remove = [
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]

        for probes_file in probes_files:
            df = pd.read_csv(probes_file, sep="\t")
            trimmed_df = df.drop(columns=columns_to_remove, errors="ignore")

            trimmed_file_path = probes_file.with_name(f"{probes_file.stem}_trimmed.tsv")
            trimmed_df.to_csv(trimmed_file_path, sep="\t", index=False)

class CleanFiles(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849')
    output_dir = luigi.Parameter(default="data")

    def requires(self):
        return TrimProbesTable(dataset_name=self.dataset_name, output_dir=self.output_dir)

    def output(self):
        return luigi.LocalTarget(
            Path(self.output_dir) / self.dataset_name / "cleanup.txt"
        )

    def run(self):
        main_dir = Path(self.output_dir) / self.dataset_name
        deleted_files = []

        for txt_file in main_dir.rglob("*.txt"):
            txt_file.unlink()
            deleted_files.append(txt_file)

        for gz_file in main_dir.rglob("*.gz"):
            gz_file.unlink()
            deleted_files.append(gz_file)

        for dir_path in main_dir.glob("**/"):
            if not any(dir_path.iterdir()):
                dir_path.rmdir()

        with self.output().open("w") as f:
            f.write("Cleanup completed.\n")

if __name__ == "__main__":
    luigi.run()