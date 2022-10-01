#!/usr/bin/env python3
"""
Convert ticker-based files from https://stooq.com/db/h/ into date-based files
"""

import argparse
from collections import namedtuple, defaultdict
from collections.abc import Generator
from datetime import datetime, date
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
    """ Data Sample
    <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
    TQQQ.US,5,20211012,153000,126.17,126.62,124.75,126.35,1251346,0
    """

    v = line.split(',')
    d = dict(zip(FIELDS, v))

    dt = datetime.strptime(d['DATE'] + d['TIME'], '%Y%m%d%H%M%S')
    dt = dt.replace(tzinfo=ZoneInfo('Poland'))
    dt = dt.astimezone(ZoneInfo('US/Eastern'))

    return Record(
        d['TICKER'].replace('.US', ''),
        d['CLOSE'],
        d['VOL'],
        dt
    )


def load_files(path: Path, earliest_date: datetime) -> list:
    print(f'Looking for txt files in {path}')

    records = []
    for f in path.glob('**/*.txt'):
        print(f'Loading {f}')

        count = 0
        for line in read(f):
            d = parse(line)
            if d.datetime.date() < earliest_date:
                continue
            records.append(d)
            count += 1

        print(f'Found {count} records')

    return sorted(records, key=lambda r: r.datetime)


def write_files(path: Path, records: list[Record]):
    print(f'Exporting {len(records)} records')

    files = defaultdict(list)
    for r in records:
        filename = r.datetime.strftime('%Y-%m-%d') + '.txt'
        files[filename].append(r)

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    for filename, records in files.items():
        print(f'Writing {filename}')

        with Path(path / filename).open('w', encoding='utf-8') as f:
            for r in records:
                line = f"{r.ticker}\t{r.price}\t{r.volume}\t{r.datetime.strftime('%H:%M')}\n"
                f.write(line)
                # print(line, end='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source_dir', default=Path.home() / 'Downloads/5_us_txt', type=Path)
    parser.add_argument('-o', '--output_dir', default='output', type=Path)
    parser.add_argument('-e', '--earliest_date', default='2021-12-01', type=date.fromisoformat)
    args = parser.parse_args()

    records = load_files(args.source_dir, args.earliest_date)
    write_files(args.output_dir, records)
