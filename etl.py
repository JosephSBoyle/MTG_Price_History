import requests as r
import sqlite3
import pandas as pd
import numpy as np

from etl_static import extract_static, load_static, transform_static

PRIMARY_KEY = ['id']
STATIC_COLS = ['object', 'id', 'oracle_id', 'multiverse_ids', 'mtgo_id',
       'mtgo_foil_id', 'tcgplayer_id', 'cardmarket_id', 'name', 'lang',
       'released_at', 'uri', 'scryfall_uri', 'layout', 'highres_image',
       'mana_cost', 'cmc', 'type_line', 'oracle_text', 'power', 'toughness',
       'colors', 'color_identity', 'keywords', 'games', 'reserved', 'foil',
       'nonfoil', 'oversized', 'promo', 'reprint', 'variation', 'set',
       'set_name', 'set_type', 'set_uri', 'set_search_uri', 'scryfall_set_uri',
       'rulings_uri', 'prints_search_uri', 'collector_number', 'digital',
       'rarity', 'flavor_text', 'card_back_id', 'artist', 'artist_ids',
       'illustration_id', 'border_color', 'frame', 'full_art', 'textless',
       'booster', 'story_spotlight', 'edhrec_rank', 'image_uris.small',
       'image_uris.normal', 'image_uris.large', 'image_uris.png',
       'image_uris.art_crop', 'image_uris.border_crop', 'legalities.standard',
       'legalities.future', 'legalities.historic', 'legalities.pioneer',
       'legalities.modern', 'legalities.legacy', 'legalities.pauper',
       'legalities.vintage', 'legalities.penny', 'legalities.commander',
       'legalities.brawl', 'legalities.duel', 'legalities.oldschool',
       'related_uris.gatherer', 'related_uris.tcgplayer_decks',
       'related_uris.edhrec', 'related_uris.mtgtop8', 'printed_name',
       'printed_type_line', 'printed_text', 'all_parts', 'promo_types',
       'arena_id', 'loyalty', 'watermark', 'preview.source',
       'preview.source_uri', 'preview.previewed_at', 'frame_effects',
       'produced_mana', 'card_faces']

TEMPO_COLS = ['usd', 'usd_foil', 'eur', 'eur_foil', 'tix']


def extract_tempo(pulled_data, dt):
    card_df = pd.json_normalize(pulled_data)

    card_df.rename(columns=lambda x: x[7:] if 'prices' in x else x, inplace=True)

    card_df = card_df[PRIMARY_KEY + TEMPO_COLS]
    card_df['datetime'] = dt

    return card_df


def transform_tempo(dataframe, missing_val=np.nan):
    '''
    1) Clean Data
    2) Replace missing values with missing_val
    :param missing_val:
    :param dataframe: df from extract
    :return:
    '''
    dataframe.fillna(value=missing_val, inplace=True)
    return dataframe


def load_tempo(dataframe):
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS TEMPO
                 (id STRING,
                  usd DECIMAL, usd_foil DECIMAL, eur DECIMAL, eur_foil DECIMAL, tix DECIMAL, datetime DATETIME, 
                  PRIMARY KEY (id, datetime))''')
    conn.commit()

    # Will raise an SQL integrity error if Primary Key is violated
    dataframe.to_sql(con=conn, name='TEMPO', if_exists='append', index=False)

    cur.execute('''SELECT * FROM TEMPO''')

    return 0

# 
# E = extract()
# T = transform_tempo(E)
# L = load_tempo(T)
# print(L)


def pull_data():
    links = r.get("https://api.scryfall.com/bulk-data").json()

    keyed_links = {x['type']: x for x in links['data']}

    all_cards = keyed_links['all_cards']
    dt = all_cards['updated_at']

    card_data = r.get(all_cards['download_uri'])
    return card_data.json(), dt


if __name__ == '__main__':
    '''
    1) pull data
    2) ETL tempo
    3) ETL static
    '''
    #  1
    data, update_dt = pull_data()
    print("Data successfully pulled from Scryfall")
    
    #  2
    tempo_df = extract_tempo(pulled_data=data, dt=update_dt)
    print("Tempo data Extracted..")
    tempo_df = transform_tempo(tempo_df)
    print("Tempo data Transformed..")
    _ = load_tempo(tempo_df)
    print("Tempo data Loaded..")
    
    #  3
    static_df = extract_static(pulled_data=data)
    print("Static data Extracted..")
    static_df = transform_static(static_df, as_str=True)
    print("Static data Transformed..")
    _ = load_static(static_df)
    print("Static data Loaded..")
