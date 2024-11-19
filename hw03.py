from pyspark.sql import SparkSession

# Starting a Spark session
spark = SparkSession.builder.appName("MyHomeworkSandbox").getOrCreate()

# Importing the dataset
products_df = spark.read.csv('./data/products.csv', header=True)
purchases_df = spark.read.csv('./data/purchases.csv', header=True)
users_df = spark.read.csv('./data/users.csv', header=True)


# # Displaying the first 10 values
# show_rows = 5

# products_df.show(show_rows)
# purchases_df.show(show_rows)
# users_df.show(show_rows)

# Creating a temporary view
products_df.createTempView("products_view")
purchases_df.createTempView("purchases_view")
users_df.createTempView("users_view")

# Queries
# Q_02
query_for_not_null_purchases = " \
    SELECT * \
    FROM purchases_view purc_v \
    WHERE \
      (purc_v.purchase_id IS NOT NULL)\
      AND (purc_v.user_id IS NOT NULL)\
      AND (purc_v.product_id IS NOT NULL)\
      AND (date IS NOT NULL)\
      AND (quantity IS NOT NULL)\
    "
# spark.sql(query_for_not_null_purchases).show()
purchases_clean_df = spark.sql(query_for_not_null_purchases)
purchases_clean_df.createOrReplaceTempView('purchases_clean')

query_for_not_null_products = " \
    SELECT * \
    FROM products_view prod_v \
    WHERE \
      (product_id IS NOT NULL)\
      AND (product_name IS NOT NULL)\
      AND (category IS NOT NULL)\
      AND (price IS NOT NULL)\
    "
# spark.sql(query_for_not_null_products).show()
products_clean_df = spark.sql(query_for_not_null_products)
products_clean_df.createOrReplaceTempView('products_clean')

query_for_not_null_users = " \
    SELECT * \
    FROM users_view user_v \
    WHERE \
      (user_id IS NOT NULL)\
      AND (name IS NOT NULL)\
      AND (age IS NOT NULL)\
      AND (email IS NOT NULL)\
    "
# spark.sql(query_for_not_null_users).show()
users_clean_df = spark.sql(query_for_not_null_users)
users_clean_df.createOrReplaceTempView('users_clean')

query_for_not_null_all = " \
    SELECT * \
    FROM purchases_clean purc_v \
    LEFT JOIN products_clean prod_v ON purc_v.product_id == prod_v.product_id \
    LEFT JOIN users_clean user_v ON purc_v.user_id == user_v.user_id \
    "

# Q_03
query_for_purchases_by_category = " \
    SELECT \
      distinct(category)\
      ,COUNT(purchase_id) OVER (PARTITION BY category) as count_purchases \
    FROM purchases_clean purc_v \
      JOIN products_clean prod_v ON purc_v.product_id == prod_v.product_id\
    "
# spark.sql(query_for_purchases_by_category).show()

# Q_04
query_for_purchases_by_category = " \
    SELECT \
      distinct(category)\
      ,COUNT(purchase_id) OVER (PARTITION BY category) as count_purchases \
    FROM purchases_clean purc_v \
      JOIN products_clean prod_v ON purc_v.product_id == prod_v.product_id\
      JOIN users_clean user_v ON purc_v.user_id == user_v.user_id\
    WHERE \
      age BETWEEN 18 AND 25 \
    "
# spark.sql(query_for_purchases_by_category).show()

# Q_05
query_for_purchases_by_category = " \
    WITH purch_by_category AS (\
    SELECT \
      distinct(category)\
      ,ROUND(SUM(price * quantity) OVER (PARTITION BY category), 2) as sum_purchases \
    FROM purchases_clean purc_v \
      JOIN products_clean prod_v ON purc_v.product_id == prod_v.product_id\
      JOIN users_clean user_v ON purc_v.user_id == user_v.user_id\
    WHERE \
      age BETWEEN 18 AND 25 \
    ) \
    SELECT \
      category \
      , ROUND((SUM(sum_purchases) * 100.0 / SUM(SUM(sum_purchases)) OVER ()), 2) as percentage\
    FROM purch_by_category\
    GROUP BY category\
    "
# spark.sql(query_for_purchases_by_category).show()

# Q_06
query_for_purchases_by_category = " \
    WITH purch_by_category AS (\
    SELECT \
      distinct(category)\
      ,ROUND(SUM(price * quantity) OVER (PARTITION BY category), 2) as sum_purchases \
    FROM purchases_clean purc_v \
      JOIN products_clean prod_v ON purc_v.product_id == prod_v.product_id\
      JOIN users_clean user_v ON purc_v.user_id == user_v.user_id\
    WHERE \
      age BETWEEN 18 AND 25 \
    ) \
    SELECT \
      category \
      , ROUND((SUM(sum_purchases) * 100.0 / SUM(SUM(sum_purchases)) OVER ()), 2) as percentage\
    FROM purch_by_category\
    GROUP BY category\
    SORT BY 2 desc\
    LIMIT 3\
    "
spark.sql(query_for_purchases_by_category).show()


spark.stop()