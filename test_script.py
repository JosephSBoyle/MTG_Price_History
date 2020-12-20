# from datetime import datetime
# ex = '2020-12-18T10:05:47.977+00:00'
# import dateutil.parser
# yourdate = dateutil.parser.parse(ex)
# print(yourdate)
# exit()
# import sqlite3
# from pprint import pprint
# conn = sqlite3.connect('database.db')
# cur = conn.cursor()
# cur.execute('''SELECT * FROM TEMPO''')
# pprint(cur.fetchall())

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

print(f'''CREATE TABLE IF NOT EXISTS STATIC 
                 ({', '.join([x + ' TEXT' for x in STATIC_COLS])} 
                  PRIMARY KEY id)''')
