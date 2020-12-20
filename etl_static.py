"""ETL sequence for updating static information about each card."""
import requests as r
import pandas as pd
import numpy as np
import json
import sqlite3

TEMPO_COLS = ['usd', 'usd_foil', 'eur', 'eur_foil', 'tix']
# STATIC_COLS = ['object', 'id', 'oracle_id', 'multiverse_ids', 'mtgo_id',
#        'mtgo_foil_id', 'tcgplayer_id', 'cardmarket_id', 'name', 'lang',
#        'released_at', 'uri', 'scryfall_uri', 'layout', 'highres_image',
#        'mana_cost', 'cmc', 'type_line', 'oracle_text', 'power', 'toughness',
#        'colors', 'color_identity', 'reserved', 'foil',
#        'nonfoil', 'oversized', 'promo', 'reprint', 'variation', 'set',
#        'set_name', 'set_type', 'set_uri', 'set_search_uri', 'scryfall_set_uri',
#        'rulings_uri', 'prints_search_uri', 'collector_number', 'digital',
#        'rarity', 'flavor_text', 'card_back_id', 'artist', 'artist_ids',
#        'illustration_id', 'border_color', 'frame', 'full_art', 'textless',
#        'booster', 'story_spotlight', 'edhrec_rank', 'image_uris.small',
#        'image_uris.normal', 'image_uris.large', 'image_uris.png',
#        'image_uris.art_crop', 'image_uris.border_crop', 'legalities.standard',
#        'legalities.future', 'legalities.historic', 'legalities.pioneer',
#        'legalities.modern', 'legalities.legacy', 'legalities.pauper',
#        'legalities.vintage', 'legalities.penny', 'legalities.commander',
#        'legalities.brawl', 'legalities.duel', 'legalities.oldschool',
#        'related_uris.gatherer', 'related_uris.tcgplayer_decks',
#        'related_uris.edhrec', 'related_uris.mtgtop8', 'printed_name',
#        'printed_type_line', 'printed_text', 'all_parts', 'promo_types',
#        'arena_id', 'loyalty', 'watermark', 'preview.source',
#        'preview.source_uri', 'preview.previewed_at', 'frame_effects',
#        'produced_mana', 'card_faces']

STATIC_COLS = ['object', 'id', 'oracle_id', 'mtgo_id',
       'mtgo_foil_id', 'tcgplayer_id', 'cardmarket_id', 'name', 'lang',
       'released_at', 'uri', 'scryfall_uri', 'layout', 'highres_image',
       'mana_cost', 'cmc', 'type_line', 'oracle_text', 'power', 'toughness',
       'colors', 'color_identity', 'keywords', 'games', 'reserved', 'foil',
       'nonfoil', 'oversized', 'promo', 'reprint', 'variation', 'set',
       'set_name', 'set_type', 'set_uri', 'set_search_uri', 'scryfall_set_uri',
       'rulings_uri', 'prints_search_uri', 'collector_number', 'digital',
       'rarity', 'flavor_text', 'card_back_id', 'artist',
       'illustration_id', 'border_color', 'frame', 'full_art', 'textless',
       'booster', 'story_spotlight', 'edhrec_rank', 'image_uris.small',
       'image_uris.normal', 'image_uris.large', 'image_uris.png',
       'image_uris.art_crop', 'image_uris.border_crop', 'legalities.standard',
       'legalities.future', 'legalities.historic', 'legalities.pioneer',
       'legalities.modern', 'legalities.legacy', 'legalities.pauper',
       'legalities.vintage', 'legalities.penny', 'legalities.commander',
       'legalities.brawl', 'legalities.duel', 'legalities.oldschool',
       'printed_name', 'printed_type_line', 'printed_text', 'arena_id', 'loyalty', 'watermark', 'preview.source',
       'preview.source_uri', 'preview.previewed_at', 'frame_effects',
       'produced_mana', 'card_faces']


def extract_static(pulled_data):
    # name, set, setcode, release date, manacost etc.

    #assert card_data.status_code == 200
    card_df = pd.json_normalize(pulled_data)
    with pd.option_context('display.max_rows', 300, 'display.max_columns', 97, 'display.expand_frame_repr', False):
        print(card_df)
    return card_df


def transform_static(dataframe, missing_val=np.nan, as_str=False):
    '''
    1) Clean Data
    2) Replace missing values with missing_val
    :param missing_val:
    :param dataframe: df from extract_static
    :return:
    '''
    dataframe.fillna(value=missing_val, inplace=True)
    if as_str is True:
        dataframe = dataframe.astype(str)
    return dataframe


def load_static(dataframe):
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    statement = f'''CREATE TABLE IF NOT EXISTS STATIC 
                 ({' '.join([f'[{x}],' for x in STATIC_COLS])}
                  PRIMARY KEY (id))'''


    print(statement)
    cur.execute(statement)
    conn.commit()


    [print(x) for x in dataframe.dtypes]
    # Will raise an SQL integrity error if Primary Key is violated
    dataframe[STATIC_COLS].to_sql(con=conn, name='STATIC', if_exists='replace', index=False)

    print(pd.read_sql('''SELECT * FROM STATIC''', conn))

    return 0
#
#
# E = extract_static()
# T = transform_static(E)
# L = load_static(T)
