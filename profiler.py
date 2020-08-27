import glob, subprocess, random, csv, os, enum, re
from collections import Counter
from operator import itemgetter

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("profiler").getOrCreate()
sc = spark.sparkContext

ROOT_LAKE_DIR = "/opt/spark/data/lake"
LAKE = "courses"
LAKE_DIR = "{}/{}".format(ROOT_LAKE_DIR, LAKE)

#class Type(enum.Flag):
#    String = 1
#    Numeric = 1 << 1
#    Categorical = 1 << 2
#    Textual = 1 << 3
#    Datetime = 1 << 4
#
#    @staticmethod
#    def deserialize(string):
#        s = string.replace('Type.', '')
#        types = s.split('|')
#        itype = Type.String
#        for type in types:
#            if type == 'Textual':
#                itype = itype | Type.Textual
#            elif type == 'Categorical':
#                itype = itype | Type.Categorical
#            elif type == 'Numeric':
#                itype = itype | Type.Numeric
#            elif type == 'Datetime':
#                itype = itype | Type.Datetime
#        return itype

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def infer_type(i, rows_list, column):
    inferred_type = 'String'
    if rows_list:
        if is_number(rows_list[0]):
            inferred_type = 'Numeric'
        try:
            if sum(1 for i in rows_list if re.match('\d{4}-\d{2}-\d{2}', i)) == len(rows_list):
                inferred_type = 'Datetime'
            elif sum(1 for i in rows_list if re.match('\d{1,2}/\d{1,2}/\d{4}', i)) == len(rows_list):
                inferred_type = 'Datetime'
        except AttributeError:
            pass

        if sum(map(len, rows_list)) / len(rows_list) > 30:
            inferred_type = 'Textual'

        if len(list(set(rows_list))) < 5:
            inferred_type = 'Categorical'
    return inferred_type

def get_common_words(final_rows_list, stop_words, drop_symbol_translator):
    word_count = dict()
    for row in final_rows_list:
        if type(row) != str or is_number(row):
            continue

        words = row.split()

        for word in words:
            processed_word = word.translate(drop_symbol_translator)
            processed_word = processed_word.lower()
            if not processed_word or processed_word not in stop_words:
                if processed_word not in word_count:
                    word_count[processed_word] = 1
                else:
                    word_count[processed_word] += 1

    return [common_word for common_word, count in Counter(word_count).most_common(10)]

def get_sample(root_lake_dir, table_name, sample_size):
    print(table_name)
    file_path = "{}/{}".format(root_lake_dir, table_name)
    stdoutdata = subprocess.check_output(['wc', '-l', file_path])
    n_lines = int(stdoutdata.split()[0])
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
        p = float(sample_size) / float(n_lines)
        for line in reader:
            if random.random() < p:
                result.append(line)
    f.close()
    return result

def delimiter_tokenize(inp, delim):
    return inp.split(delim)

def qgram_tokenize(inp, q):
    qgram_list = []
    if len(inp) < q or q < 1:
        return qgram_list
    qgram_list = [inp[i:i + q] for i in range(len(inp) - (q - 1))]
    return qgram_list

def map_table(rows_list):
    final = [map_column(i, rows_list[0], rows_list[2:], column) for i,column in enumerate(rows_list[1])]
    return final

def map_column(i, table_name, rows_list, column):
    drop_symbol_translator = str.maketrans('', '', ',./\'\"\\:;[]{}-_?!~`@#$%^&*()')
    stop_words = set(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll",\
                  "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", \
                  'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs',\
                  'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am',\
                  'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', \
                  'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', \
                  'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', \
                  'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under',\
                  'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any',\
                  'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', \
                  'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', \
                  "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', \
                  "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', \
                  "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", \
                  'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", \
                  'wouldn', "wouldn't"])
    drop_vowel_translator = str.maketrans('', '', 'aeiou')

    column_name_index = '{}/{}'.format(table_name, column)
    filtered_rows_list = map(itemgetter(i), rows_list)
    final_rows_list = list(filter(None, filtered_rows_list))
    column_lower = column.lower()
    word_tokens = delimiter_tokenize(column, "_")
    three_gram_tokens = qgram_tokenize(column, 3)
    three_gram_nv_tokens = qgram_tokenize(column_lower.translate(drop_vowel_translator), 3)
    itype = infer_type(i, final_rows_list, column)
    common_words = get_common_words(final_rows_list, stop_words, drop_symbol_translator)
    ds = final_rows_list
    sample = ds if len(ds) < 10 else random.sample(ds, 10)
    trait = list()
    trait.append(column_name_index)
    trait.append(table_name)
    trait.append(column)
    trait.append(column_lower)
    trait.append(word_tokens)
    trait.append(three_gram_tokens)
    trait.append(three_gram_nv_tokens)
    trait.append(itype)
    trait.append(common_words)
    trait.append([float(item) if is_number(item) else item for item in sample])
    return trait

def search_dir():
    files = glob.glob("{}/*.csv".format(LAKE_DIR), recursive=True)
    return list(map(lambda x: os.path.relpath(x, ROOT_LAKE_DIR), files))

tables_rdd = sc.parallelize(search_dir())
profiles_rdd = tables_rdd.flatMap(lambda x: map_table(get_sample(ROOT_LAKE_DIR, x, 1000)))
df = spark.createDataFrame(profiles_rdd)
df.show()
