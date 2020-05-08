# data-labeler
### Step 1 create a table of unlabeled data
`"CREATE VIRTUAL TABLE data USING FTS5 (text, rating)") ` 
### Step 2 create a table for the labeled data
`"CREATE TABLE labeled_data (df_index int, text text, rating text)"`
### Step 3 Populate unlabeled database
`conn.executemany("INSERT INTO data VALUES (?,?)", chunk)`

### Step 4 Customize labels in labels.py

### Step 5 run app
`waitress-serve api:app`

