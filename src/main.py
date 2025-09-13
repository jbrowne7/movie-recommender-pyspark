from pyspark.sql import SparkSession
from data_loader import load_data

def start():

    ratings_path='../data/u.data'
    movies_path='../data/u.item'
    spark = SparkSession.builder.appName("MovieRecommender").getOrCreate()

    ratings_df, movies_df = load_data(spark, ratings_path, movies_path)
    
    while True:
        command = input("Enter command (type 'exit' to quit): ").strip()
        if command == 'exit':
            break
        elif command == 'show_ratings':
            ratings_df.show()
        elif command == 'show_movies':
            movies_df.show()
        else:
            print("Unknown command.")

if __name__ == "__main__":
    start()