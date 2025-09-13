
def load_data(spark, ratings_path, movies_path):

	# u.data columns: user id | item id | rating | timestamp
	ratings_schema = ["user_id", "item_id", "rating", "timestamp"]
	ratings_df = spark.read.csv(ratings_path, sep='\t', inferSchema=True).toDF(*ratings_schema)

	movies_schema = [
		"movie_id", "movie_title", "release_date", "video_release_date", "IMDb_URL"
	] + [f"genre_{i}" for i in range(19)]
	movies_df = spark.read.csv(movies_path, sep='|', inferSchema=True).toDF(*movies_schema)

	return ratings_df, movies_df

