import sqlite3
import numpy as np
import pandas as pd
from datetime import timedelta


def ascertain_dims(cur):
    # sample-wise
    cur.execute('''SELECT DISTINCT id FROM Tempo''')
    ids = cur.fetchall()
    print(f'There are {len(ids)} distinct cards in the database Tempo')

    # timestep-wise
    cur.execute('''SELECT DISTINCT datetime from Tempo''')
    dts = [x[0] for x in cur.fetchall()]
    print(f'There are {len(dts)} distinct timesteps in the database Tempo')

    # feature-wise
    cur.execute('''PRAGMA table_info(Tempo);''')
    fts = [x[0] for x in cur.fetchall()]
    print(f'There are {len(fts)} distinct features in the database Tempo')

    return ids, dts, fts


def build_timestep(cur, ids, datetime, features):
    cur.execute(f'SELECT usd, usd_foil, eur, eur_foil, tix FROM Tempo WHERE datetime="{datetime}"')
    resp = np.array(cur.fetchall())
    print(resp)
    timestep_id, timestep = resp[:, :1], resp[:, 2:]
    timestep_id = timestep_id[:, 0]

    id_index = {}  # mapping between id and numpy index
    for x, y in zip(timestep_id, [i for i, j in enumerate(timestep_id)]):
        id_index[x] = y

    nrows, ncols = len(ids), len(features) - 2
    matrix = np.zeros(nrows * ncols).reshape(nrows, ncols)  # array is by default filled with minus ones (missing)

    # for each index and sample in Static:
    # find the corresponding timestep index using the sharedkey CardId
    # assign that index in the matrix to timestep's values for said CardId
    for i, id in enumerate(ids):
        try:
            matrix[i] = timestep[id_index[id]]
        except Exception as e:
            # print(e)
            pass
    return matrix


def nearest_date(items, pivot):
    nearest = min(items, key=lambda x: abs(x - pivot))
    timedelta = abs(nearest - pivot)
    return nearest, timedelta


if __name__ == '__main__':
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()

    ids, dts, fts = ascertain_dims(cur)
    n_ids, n_dts, n_fts = len(ids), len(dts), len(fts)

    final_matrix = np.zeros(shape=(n_ids, n_dts, n_fts-2))

    first = dts[0]
    last = dts[-1]

    # account for timesteps w/out data
    all_timesteps = pd.date_range(first, last, freq='D')
    missing = []
    print(all_timesteps)

    for i, t in enumerate(all_timesteps):

        matched_time, difference = nearest_date(all_timesteps, t)
        if difference <= timedelta(hours=6):
            print(f"Timestep {t} matched with data collected at {matched_time}")
            final_matrix[:, i, :] = build_timestep(cur, ids, dts[i], fts)
            missing.append(0)
        else:
            missing.append(1)
    np.save(arr=final_matrix, file=f'3d.npy')
