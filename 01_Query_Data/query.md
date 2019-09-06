
## Query data : SQL, Hive, Pandas, MongoDB

#### author : lhphong@tma

### 1. Create database


```python
# SQL
CREATE SCHEMA IF NOT EXISTS 'name_database' DEFAULT CHARACTER SET utf8;
USE 'name_database';

# Hive
CREATE SCHEMA IF NOT EXISTS 'name_database';
USE 'name_database';

# MongoDB
use 'NAME_DATABASE';

# Pandas
df = pd.DataFrame({'name_table' : ['column 1', 'column 2', 'column 3']})
```

### 2. Drop database


```python
# SQL + Hive 
DROP 'name_database'

# MongoDB
use 'name_database'
db.dropDatabase()
```

### 3. SELECT, WHERE, DISTINCT, LIMIT


```python
# SQL 
SELECT * FROM name_database
SELECT id FROM name_database WHERE colum_name = 'keyword'
SELECT DISTINCT column1, column2, ... FROM table_name;
SELECT * FROM name_databse LIMIT 10

# Hive

# MongoDB

# Pandas
name_data
name_data[name_data.column_name == 'keyword'].id
name_data.colum1.unique()
name_data.head(10)
```



### 4. SELECT with multiple conditions


```python
# SQL
SELECT * FROM name_database WHERE name = 'key' and value = 'var'
SELECT id, name, value FROM name_database WHERE name = 'key' and value = 'var'

# Hive

# MongoDB

# Pandas
name_data[(name_data.name == 'key') & (name_data.value)]
name_data[(name_data.name == 'key') & (name_data.value)].[['id', 'name', 'value']]
```

## 5. ORDER BY


```python
# SQL
SELECT * FROM name_data where airport_ident = 'KLAX' order by type
SELECT * FROM name_data where airport_ident = 'KLAX' order by type desc

# Pandas
name_data[name_data.airport_ident == 'KLAX'].sort_values('type')
name_data[name_data.airport_ident == 'KLAX'].sort_values('type', ascending=False)
```


## 6. IN… NOT IN


```python
# SQL
select * from airports where type in ('heliport', 'balloonport')
select * from airports where type not in ('heliport', 'balloonport')

# Pandas
airports[airports.type.isin(['heliport', 'balloonport'])]
airports[~airports.type.isin(['heliport', 'balloonport'])]
```



## 7. GROUP BY, COUNT, ORDER BY


```python
# SQL
select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type
select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc

select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type
select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc

# Pandas
airports.groupby(['iso_country', 'type']).size()
airports.groupby(['iso_country', 'type']).size().to_frame('size').reset_index().sort_values(['iso_country', 'size'], ascending=[True, False])

airports.groupby(['iso_country', 'type']).size()
airports.groupby(['iso_country', 'type']).size().to_frame('size').reset_index().sort_values(['iso_country', 'size'], ascending=[True, False])
```




## 8. HAVING


```python
# SQL
select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc

# Pandas
airports[airports.iso_country == 'US'].groupby('type').filter(lambda g: len(g) > 1000).groupby('type').size().sort_values(ascending=False)
```



## 9. Top N records


```python
# SQL
select iso_country from by_country order by size desc limit 10
select iso_country from by_country order by size desc limit 10 offset 10

# Pandas
by_country.nlargest(10, columns='airport_count')
by_country.nlargest(10, columns='airport_count')
```



## 10. Aggregate functions (MIN, MAX, MEAN)


```python
# SQL
select max(length_ft), min(length_ft), mean(length_ft), median(length_ft) from runways

# Pandas
runways.agg({'length_ft': ['min', 'max', 'mean', 'median']})
```




## 11. JOIN


```python
# SQL
select airport_ident, type, description, frequency_mhz 
from airport_freq join airports on airport_freq.airport_ref = airports.id 
where airports.ident = 'KLAX'

# Pandas
airport_freq.merge(airports[airports.ident == 'KLAX'][['id']], left_on='airport_ref', right_on='id', how='inner')[['airport_ident', 'type', 'description', 'frequency_mhz']]
```




## 12. UNION ALL and UNION


```python
# SQL
select name, municipality from airports where ident = 'KLAX' 
union all select name, municipality from airports where ident = 'KLGB'

# Pandas
pd.concat([airports[airports.ident == 'KLAX'][['name', 'municipality']], 
           airports[airports.ident == 'KLGB'][['name', 'municipality']]])
```
    


## 13. INSERT


```python
# SQL
create table heroes (id integer, name text);
insert into heroes values (1, 'Harry Potter');
insert into heroes values (2, 'Ron Weasley');
insert into heroes values (3, 'Hermione Granger');

# Hive
CREATE EXTERNAL TABLE offers (
    offerid INT,
    category INT,
    quantity INT,
    company INT,
    offervalue DOUBLE,
    brand INT
);
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1"); 

# Pandas
df1 = pd.DataFrame({'id': [1, 2], 'name': ['Harry Potter', 'Ron Weasley']})
df2 = pd.DataFrame({'id': [3], 'name': ['Hermione Granger']})
...
pd.concat([df1, df2]).reset_index(drop=True)
```



## 14. UPDATE


```python
# SQL 
update airports set home_link = 'http://www.lawa.org/welcomelax.aspx' where ident == 'KLAX'

# Pandas
airports.loc[airports['ident'] == 'KLAX', 'home_link'] = 'http://www.lawa.org/welcomelax.aspx'

```



## 15. DELETE


```python
# SQL
delete from lax_freq where type = 'MISC'

# Pandas
lax_freq = lax_freq[lax_freq.type != 'MISC']
lax_freq.drop(lax_freq[lax_freq.type == 'MISC'].index)
```



## 16. And more!


```python
# Pandas add new column 
df['total_cost'] = df['price'] * df['quantity']

# Pandas to...
df.to_csv(...)  # csv file
df.to_hdf(...)  # HDF5 file
df.to_pickle(...)  # serialized object
df.to_sql(...)  # to SQL database
df.to_excel(...)  # to Excel sheet
df.to_json(...)  # to JSON string
df.to_html(...)  # render as HTML table
df.to_feather(...)  # binary feather-format
df.to_latex(...)  # tabular environment table
df.to_stata(...)  # Stata binary data files
df.to_msgpack(...)	# msgpack (serialize) object
df.to_gbq(...)  # to a Google BigQuery table.
df.to_string(...)  # console-friendly tabular output.
df.to_clipboard(...) # clipboard that can be pasted into Excel

# Pandas plot it
top_10.plot(
    x='iso_country', 
    y='airport_count',
    kind='barh',
    figsize=(10, 7),
    title='Top 10 countries with most airports')


```
