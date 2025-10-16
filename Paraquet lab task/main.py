import pyarrow as pa
import pyarrow.parquet as pq


data = {
    "Name": ["Suheera", "khola", "Rubab"],
    "Age": [21, 20,22],
    "City": ["Karachi", "Lahore","Islamabad"]
}


table = pa.table(data)

pq.write_table(table, "people.parquet")

print("Parquet file created successfully with 2 entries!")

table = pq.read_table("people.parquet")
print(table.to_pandas())
