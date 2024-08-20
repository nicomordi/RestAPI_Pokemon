import requests
import pandas as pd
import psycopg2
import os

def get_pokemon_data(pokemon_id_or_name):
    url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id_or_name}/'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def normalize_pokemon_data(data):
    normalized_data = {
        'id': data['id'],
        'name': data['name'],
        'base_experience': data['base_experience'],
        'height': data['height'],
        'weight': data['weight'],
        'order': data['order'],
        'is_default': data['is_default'],
        'abilities': ', '.join([ability['ability']['name'] for ability in data['abilities']]),
        'types': ', '.join([type_['type']['name'] for type_ in data['types']])
    }
    return normalized_data

# Obtener datos de varios Pokémon
pokemon_ids_or_names = ['pikachu', 'bulbasaur', 'charmander', 'squirtle']  # Puedes agregar más
pokemon_data_list = []

for pokemon_id_or_name in pokemon_ids_or_names:
    data = get_pokemon_data(pokemon_id_or_name)
    if data:
        normalized_data = normalize_pokemon_data(data)
        pokemon_data_list.append(normalized_data)

# Convertir la lista de datos normalizados en un DataFrame de pandas
df = pd.DataFrame(pokemon_data_list)

# Extraer un archivo .csv que se guarda en nuestra computadora
csv_file = 'pokemon_data.csv'
df.to_csv(csv_file, index=False)

print("Directorio actual:", os.getcwd())
print(df)

# Conectarse a la base de datos de Redshift
conn = psycopg2.connect(
    dbname='data-engineer-database',
    user='nicomordi_coderhouse',
    password='O956VwKRBB',
    host='http://data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com/ ',
    port='5439'
)
cur = conn.cursor()

# Crear tabla en Redshift (si no existe)
create_table_query = '''
CREATE TABLE IF NOT EXISTS pokemon (
    id INT,
    name VARCHAR(255),
    base_experience INT,
    height INT,
    weight INT,
    order INT,
    is_default BOOLEAN,
    abilities VARCHAR(255),
    types VARCHAR(255)
);
'''
cur.execute(create_table_query)
conn.commit()

with open(csv_file, 'r') as f:
    next(f)  # Omitir el encabezado
    cur.copy_from(f, 'pokemon', sep=',', null='')

conn.commit()
cur.close()
conn.close()

