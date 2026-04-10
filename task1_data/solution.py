import re
import ast
import sqlite3


with open("task1_d.json", "r", encoding="utf-8") as f:
    content = f.read() # это для того чтобы открыть наш афйл и получается будем читать полнотсью как текст

content = re.sub(r':(\w+)=>', r'"\1":', content)# это потому что у нас  в файле данные написаны не в таком формате как нам нужно и меняет данные на верный формат
books = ast.literal_eval(content) # для работы в питоне
conn = sqlite3.connect("task1.db")
cur = conn.cursor() #создаем нашу дб

cur.execute("""
CREATE TABLE IF NOT EXISTS raw_books (
    id TEXT PRIMARY KEY,
    title TEXT,
    author TEXT,
    genre TEXT,
    publisher TEXT,
    publication_year INTEGER,
    price_raw TEXT
)
""")
cur.execute("DELETE FROM raw_books")#Чтобы не было дублей и ошибо  мы очищаем таблицу перед новой загрузко

for book in books:
    cur.execute("""
        INSERT INTO raw_books (
            id, title, author, genre, publisher, publication_year, price_raw
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        str(book["id"]),
        book["title"],
        book["author"],
        book["genre"],
        book["publisher"],
        int(book["year"]),
        book["price"]
    ))

conn.commit()#Сохраняем изменения в базе.
cur.execute("DROP TABLE IF EXISTS books_summary")
cur.execute("""
CREATE TABLE books_summary AS
SELECT
    publication_year,
    COUNT(*) AS book_count,
    ROUND(
        AVG(
            CASE
                WHEN price_raw LIKE '$%' THEN CAST(SUBSTR(price_raw, 2) AS REAL)
                WHEN price_raw LIKE '€%' THEN CAST(SUBSTR(price_raw, 2) AS REAL) * 1.2
            END
        ),
        2
    ) AS average_price_usd
FROM raw_books
GROUP BY publication_year
ORDER BY publication_year
""")

conn.commit()
raw_count = cur.execute("SELECT COUNT(*) FROM raw_books").fetchone()[0]
print("Rows in raw_books:", raw_count)
summary_count = cur.execute("SELECT COUNT(*) FROM books_summary").fetchone()[0]
print("Rows in books_summary:", summary_count)

print("\nFirst 10 rows from books_summary:")
for row in cur.execute("SELECT * FROM books_summary LIMIT 10"):
    print(row)

conn.close()#Закрываем соединение с базой