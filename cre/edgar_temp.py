import pandas,re,fs_datatools

filename = "n257x10_424b5.htm"


text = open(filename,'r').read()
interesting = ['SF','square feet','sqft','noi','net operating income','rent','lease','occupancy','property type','year built','address','units','tenant']

all_tables = [item for item in re.findall(r'<table.+?/table>',text,re.DOTALL) if item.count('td') > 100]
filtered_tables = []

for table in all_tables:
     df  = pandas.read_html(table,header=0)[0]
     if fs_datatools.are_any_in_df(df,interesting):
         filtered_tables.append(df)

for df_f in filtered_tables:
    print(df_f)
