import os
import shutil
import kagglehub


path = kagglehub.dataset_download("zadafiyabhrami/global-crocodile-species-dataset")
print("Dataset downloaded to:", path)


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_dir = os.path.join(project_root, "csv")
os.makedirs(csv_dir, exist_ok=True)

for file in os.listdir(path):
    if file.endswith(".csv"):
        src = os.path.join(path, file)
        dst = os.path.join(csv_dir, "crocodile_species.csv")
        shutil.copy(src, dst)
        print(f"Copied {src} -> {dst}")
        break
else:
    raise FileNotFoundError("No CSV file found in Kaggle dataset.")