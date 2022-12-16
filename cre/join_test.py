import pandas as pd

raw_data = {
        'subject_id': ['1', '2', '3', '4', '5','6','7'],
        'first_name': ['Alex', 'Amy', 'Allen', 'Alice', 'Ayoung','John','Stacy'], 
        'last_name': ['Anderson', 'Ackerman', 'Ali', 'Aoni', 'Atiches','Carter','Holsen']}
df_a = pd.DataFrame(raw_data, columns = ['subject_id', 'first_name', 'last_name'])

print ("DF_A\n")
print (df_a)

raw_data = {
        'subject_id': ['3', '1', '2', '4', '5','8','9','10'],
        'favorite_toy': ['boat', 'train', 'ball', 'cup', 'box','squirrel','book','chair'], 
        'favorite_soap': ['cocubut', 'blackberry', 'almond', 'lemon', 'mountain fresh','vanilla','lemongrass','berry']}
df_b = pd.DataFrame(raw_data, columns = ['subject_id','favorite_toy', 'favorite_soap'])



print ("\nDF_B\n")
print (df_b)

print ("joined")

print (df_a.join(df_b.set_index('subject_id'), on='subject_id'))
print (df_a)
