import os

files = os.listdir('../Electricity')
for filename in files:
    name_list = filename.split('_')
    if name_list[1] != 'electricity':
        name_list[0] = name_list[0][:-11]
        name_list.append(name_list[1])
        name_list[1] = 'electrcity'
        new_name = '_'.join(name_list)
        os.rename(f"../Electricity/{filename}", f"../Electricity/{new_name}")