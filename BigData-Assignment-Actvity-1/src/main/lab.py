import pandas as pd
import sqlite3

def process_data():

    # Loading the transactions data from the CSV file into a pandas DataFrame
    file_path = r"src/data/transactions.csv" 
    df = pd.read_csv(file_path, encoding="utf-8")
    
    # Removing any rows with missing values in the DataFrame (Use dropna or another method)
    df.dropna(inplace=True)  # You can change this to other methods if required

    # Converting the 'TransactionDate' column to a datetime format using pandas
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

    # Setting up a connection to SQLite database and create a table if it doesn't exist
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product TEXT,
        amount REAL,
        TransactionDate TEXT,
        PaymentMethod TEXT,
        City TEXT,
        Category TEXT
    )
    """)
    
    # TO DO: Insert data into the database
    # Your task: Insert the cleaned DataFrame into the SQLite database. Ensure to replace the table if it already exists.
    df.to_sql("transactions",conn,if_exists="replace",index=False)

    # Example Queries - Write SQL queries based on the instructions below

    # TO DO: Query for Top 5 Most Sold Products
    # Your task: Write an SQL query to find the top 5 most sold products based on transaction count.
    cursor.execute(""" select product ,count(*) as transaction_count from transactions group by product order by transaction_count desc limit 5 """)
    print("\nTop 5 Most Sold Products:\n",cursor.fetchall())


    # TO DO:  Query for Monthly Revenue Trend
    # Your task: Write an SQL query to find the total revenue per month.
    cursor.execute("""  select strftime('%Y-%m',TransactionDate) as month , sum(amount) as total_revenue from transactions group by month order by month """)
    print("\nMonthly Revenue Trend:\n",cursor.fetchall())

    # TO DO:  Query for Payment Method Popularity
    # Your task: Write an SQL query to find the popularity of each payment method used in transactions.
    cursor.execute("""  select PaymentMethod, count(*) as usage_count from transactions group by PaymentMethod oder by usage_count desc """)
    print("\nPayment Method Popularity:\n",cursor.fetchall())
    

    # TO DO:  Query for Top 5 Cities with Most Transactions
    # Your task: Write an SQL query to find the top 5 cities with the most transactions.
    cursor.execute("""
    SELECT City, COUNT(*) AS transaction_count
    FROM transactions
    GROUP BY City
    ORDER BY transaction_count DESC
    LIMIT 5
    """)
    print("\nTop 5 Cities with Most Transactions:\n", cursor.fetchall())


    # TO DO:  Query for Top 5 High-Spending Customers
    # Your task: Write an SQL query to find the top 5 customers who spent the most in total.
    cursor.execute("""
    SELECT customer_id, SUM(amount) AS total_spent
    FROM transactions
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 5
    """)
    print("\nTop 5 High-Spending Customers:\n", cursor.fetchall())

    # TO DO:  Query for Hadoop vs Spark Related Product Sales
    # Your task: Write an SQL query to categorize products related to Hadoop and Spark and find their sales.
    cursor.execute("""
    SELECT 
        CASE 
            WHEN product LIKE '%Hadoop%' THEN 'Hadoop'
            WHEN product LIKE '%Spark%' THEN 'Spark'
            ELSE 'Other'
        END AS category,
        COUNT(*) AS sales_count,
        SUM(amount) AS total_revenue
    FROM transactions
    WHERE product LIKE '%Hadoop%' OR product LIKE '%Spark%'
    GROUP BY category
    """)
    print("\nHadoop vs Spark Related Product Sales:\n", cursor.fetchall())


    # TO DO:  Query for Top Spending Customers in Each City
    # Your task: Write an SQL query to find the top spending customer in each city using subqueries.
    cursor.execute("""
    SELECT City, customer_id, MAX(total_spent) AS max_spent
    FROM (
        SELECT City, customer_id, SUM(amount) AS total_spent
        FROM transactions
        GROUP BY City, customer_id
    ) AS city_customer_totals
    GROUP BY City
    """)
    print("\nTop Spending Customers in Each City:\n", cursor.fetchall())


    # Step 8: Close the connection
    # Your task: After all queries, make sure to commit any changes and close the connection
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & Advanced Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
