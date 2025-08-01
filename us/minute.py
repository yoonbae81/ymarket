#!/usr/bin/env python3
"""
Convert ticker-based files from https://stooq.com/db/h/ into date-based files
(Performance improved with parallel processing)
"""

import argparse
from collections import namedtuple, defaultdict
from collections.abc import Generator
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo  # requires `pip install tzdata` on Windows
from concurrent.futures import ProcessPoolExecutor # 병렬 처리를 위해 추가
from functools import partial 
from itertools import groupby


FIELDS = ['TICKER', 'PER', 'DATE', 'TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'OPENINT']
Record = namedtuple('Record', ['ticker', 'price', 'volume', 'datetime'])

# --- 병렬 처리를 위한 헬퍼 함수 ---

def _process_single_file(file: Path, earliest_date: date) -> list[Record]:
    """하나의 파일을 읽고 파싱하여 Record 리스트를 반환 (병렬 작업용)"""
    print(f'Loading {file.name}')
    records = []
    for line in read(file):
        d = parse(line)
        if d.datetime.date() < earliest_date:
            continue
        records.append(d)
    print(f'Found {len(records)} records in {file.name}')
    return records

def _write_single_day_file(args: tuple[Path, str, list[Record]]):
    """하나의 날짜에 해당하는 데이터를 파일에 씀 (병렬 작업용)"""
    output_dir, filename, records = args
    print(f'Writing {filename}')
    with Path(output_dir / filename).open('w', encoding='utf-8') as f:
        for r in records:
            line = f"{r.ticker}\t{r.price}\t{r.volume}\t{r.datetime.strftime('%H:%M')}\n"
            f.write(line)
    return filename

# --- 기존 함수 (일부 수정) ---

def read(file: Path) -> Generator[str]:
    with file.open() as f:
        _ = f.readline()  # header line
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
    """Phase 1: 여러 소스 파일을 병렬로 읽어들임"""
    print(f'Looking for txt files in {path}')
    files_to_process = list(path.glob('**/*.txt'))
    print(f'Found {len(files_to_process)} files to process in parallel.')

    all_records = []
    # ProcessPoolExecutor를 사용하여 여러 파일을 동시에 읽고 파싱
    with ProcessPoolExecutor() as executor:
        # functools.partial을 사용해 _process_single_file 함수에 earliest_date 인자를 고정
        process_func = partial(_process_single_file, earliest_date=earliest_date)
        
        # map을 이용해 각 파일에 process_func를 병렬로 적용
        results = executor.map(process_func, files_to_process)

        # 각 프로세스에서 반환된 레코드 리스트를 하나로 합침
        for file_records in results:
            all_records.extend(file_records)

    print(f'Total records loaded: {len(all_records)}')
    # 모든 레코드를 메모리에 올린 후 시간순으로 정렬
    return sorted(all_records, key=lambda r: r.datetime)


def write_files(path: Path, records: list[Record]):
    """Phase 2: 메모리의 레코드를 날짜별 파일에 병렬로 저장 (groupby로 최적화)"""
    print(f'Grouping {len(records)} records by date...')

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    # 그룹화의 기준이 되는 key 함수 정의 (날짜 객체 자체를 키로 사용)
    key_func = lambda r: r.datetime.date()

    # 1. itertools.groupby로 레코드 그룹화
    # groupby는 정렬된 데이터를 매우 빠르게 그룹화합니다.
    # (날짜, 해당 날짜의 레코드 이터레이터) 형태의 튜플을 생성합니다.
    grouped_records = groupby(records, key=key_func)

    # 2. 병렬 쓰기를 위한 인자 생성
    # 각 그룹에서 파일명과 레코드 리스트를 추출하여 병렬 처리 함수에 전달할 인자를 만듭니다.
    write_args = []
    for day_date, record_group in grouped_records:
        filename = day_date.strftime('%Y-%m-%d') + '.txt'
        # record_group은 이터레이터이므로 list로 변환해야 함
        write_args.append((path, filename, list(record_group)))
    
    print(f'Exporting {len(write_args)} daily files in parallel...')

    # 3. 병렬로 파일 쓰기 
    with ProcessPoolExecutor() as executor:
        for filename in executor.map(_write_single_day_file, write_args):
            print(f'Finished writing {filename}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source_dir', default=Path.home() / 'Downloads/5_us_txt', type=Path)
    parser.add_argument('-o', '--output_dir', default='output', type=Path)
    parser.add_argument('-e', '--earliest_date', default='2021-12-01', type=date.fromisoformat)
    args = parser.parse_args()

    # Phase 1과 2는 순차적으로 실행
    records = load_files(args.source_dir, args.earliest_date)
    write_files(args.output_dir, records)