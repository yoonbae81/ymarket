#!/usr/bin/env python3
"""
Convert ticker-based files from https://stooq.com/db/h/ into date-based files
"""

import argparse
from collections import namedtuple, defaultdict
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo  # requires `pip install tzdata` on Windows

FIELDS = ['TICKER', 'PER', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OPENINT']
Record = namedtuple('Record', ['ticker', 'price', 'volume', 'datetime'])


def read(file: Path) -> Generator[str]:
    with file.open() as f:
        # discard the first line containing headers
        _ = f.readline()

        while line := f.readline():
            yield line


def parse(line: str) -> Record:
    values = line.split(',')
    d = dict(zip(FIELDS, values))

    dt = datetime.strptime(d['DATE'] + d['TIME'], '%Y%m%d%H%M%S')
    dt = dt.replace(tzinfo=ZoneInfo('Poland'))
    dt = dt.astimezone(ZoneInfo('US/Eastern'))
    d['DATETIME'] = dt.isoformat()  # 2021-12-31T00:00:00-04:00

    return Record(
        d['TICKER'].replace('.US', ''),
        d['CLOSE'],
        d['VOL'],
        d['DATETIME']
    )


def load_files(path) -> list:
    print(f'Looking for txt files in {path}')

    records = []
    for f in path.glob('**/*.txt'):
        print(f'Loading {f}')

        count = 0
        for line in read(f):
            records.append(parse(line))
            count += 1

        print(f'Found {count} records')

    return sorted(records, key=lambda r: r.datetime)


def write_files(path, records):
    print(f'Exporting {len(records)} records')

    files = defaultdict(list)
    for record in records:
        filename = record.datetime[0:10] + '.txt'
        files[filename].append(record)

    for filename, records in files.items():
        print(f'Writing {filename}')

        # for r in records:
        #     print(f'{r.ticker}\t{r.price}\t{r.volume}\t{r.datetime[11:16]}')

        with Path(path / filename).open('w', encoding='utf-8') as f:
            for r in records:
                f.write(f'{r.ticker}\t{r.price}\t{r.volume}\t{r.datetime[11:16]}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', default=Path.cwd())
    parser.add_argument('-o', '--output', default=Path.cwd())
    args = parser.parse_args()

    records = load_files(args.source)
    write_files(args.output, records)


""" Sample Data
<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
TQQQ.US,5,20211012,153000,126.17,126.62,124.75,126.35,1251346,0
"""
