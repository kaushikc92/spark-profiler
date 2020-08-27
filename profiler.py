import glob, subprocess, random, csv, os

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("profiler").getOrCreate()
sc = spark.sparkContext

ROOT_LAKE_DIR = "/opt/spark/data/lake"
LAKE = "courses"
LAKE_DIR = "{}/{}".format(ROOT_LAKE_DIR, LAKE)

def get_sample(root_lake_dir, table_name, sample_size):
    print(table_name)
    file_path = "{}/{}".format(root_lake_dir, table_name)
    stdoutdata = subprocess.check_output(['wc', '-l', file_path])
    n_lines = stdoutdata.split()[0]
    try:
        f = open(file_path, encoding="utf-8")
    except UnicodeDecodeError:
        f = open(file_path, encoding="ISO-8859-1")
    result = list()
    reader = csv.reader(f)
    result.append(table_name)
    result.append(next(reader))
    if sample_size > n_lines:
        for line in reader:
            result.append(line)
    else:
        p = float(sample_size) / float(num_lines)
        for line in reader:
            if random.random() < p:
                result.append(line)
    f.close()

def map_table(rows_list):
    final = [map_column(i, rows_list[0], rows_list[2:], column) for i,column in enumerate(rows_list[1])]
    return final

def map_column(i, table_name, rows_list, column):
    trait = list()
    trait.append(table_name)
    trait.append(column)
    return trait

def search_dir():
    files = glob.glob("{}/*.csv".format(LAKE_DIR), recursive=True)
    return list(map(lambda x: os.path.relpath(x, ROOT_LAKE_DIR), files))

tables_rdd = sc.parallelize(search_dir(LAKE_DIR))
profiles_rdd = tables_rdd.flatMap(lambda x: map_table(get_sample(ROOT_LAKE_DIR, x, 1000)))
df = spark.createDataFrame(profiles_rdd)
df.show()
