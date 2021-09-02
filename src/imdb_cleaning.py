import pyspark.sql.functions as f
import pyspark.sql.types as t
from unidecode import unidecode

FILE_PATHS = {
    'local': {
        'basics': 'data/tp/title_basics.tsv',
        'ratings': 'data/tp/title_ratings.tsv'
        },
    'cloud':{
        'basics': 'gs://edc_bootamp_data/data/title_basics.tsv',
        'ratings': 'gs://edc_bootamp_data/data/title_ratings.tsv',
    }
}
SAVE_PATH = {
    'local': 'data/imdb/title_basics_with_rating',
    'cloud': 'gs://edc_bootamp_data/data/title_basics_with_rating',
}

@f.udf(returnType=t.StringType())
def unidecode_udf(string):
    if not string:
        return None
    else:
        return unidecode(string)


class ImdbCleaner:

    def __init__(self, spark_session, exec_mode):

        self.spark = spark_session
        self.exec_mode = exec_mode
        self.read_options = {
            'header': True, 
            'sep': '\t'
        }

    def read_data(self):
        
        self.df_basics = (
            self.spark
            .read
            .format('csv')
            .options(**self.read_options)
            .load(FILE_PATHS[self.exec_mode]['basics'])
        )
        self.df_ratings = (
            self.spark
            .read
            .format('csv')
            .options(**self.read_options)
            .load(FILE_PATHS[self.exec_mode]['ratings'])
        )

    def data_cleaning(self):

        self.df_cleaned = self.df_basics
        # Limpa os Inteiros
        int_cols = ['startYear', 'endYear', 'runtimeMinutes', 'isAdult']
        for c in int_cols:
            self.df_cleaned = (
                self.df_cleaned
                .withColumn(c, f.col(c).cast('int'))
            )
        # Limpa os Strings
        str_cols = ['primaryTitle', 'originalTitle', 'titleType']
        for c in str_cols:
            self.df_cleaned = (
                self.df_cleaned
                .withColumn(c, unidecode_udf(f.col(c)))
            )
        # Limpezas Específicas
        self.df_cleaned = (
            self.df_cleaned
            .replace('\\N', None)
            .withColumn('genres', f.split(f.col('genres'), ','))
        )

    def join_data(self):

        self.df_final = (
            self.df_cleaned
            .join(self.df_ratings, ['tconst'])
        )

    def write_data(self):
        
        (
            self.df_final
            .write
            .format('parquet')
            .mode('overwrite')
            .save(SAVE_PATH[self.exec_mode])
        )
    
    def clean(self):

        self.read_data()
        self.data_cleaning()
        self.join_data()
        self.write_data()