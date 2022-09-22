import csv
from datetime import datetime
from multiprocessing.sharedctypes import Value
from fastapi import FastAPI, APIRouter, Response, status

import time


app = FastAPI()

"""
GET /queries/count/2015: {"count": 573697}
GET /queries/count/2015-08: {"count": 573697}
GET /queries/count/2015-08-03: {"count": 198117}
GET /queries/count/2015-08-01 00:04: {"count": 617} Distinct queries done on Aug 1st between 00:04:00 and 00:04:59: : {"count": 617}
"""

def _to_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')


def parse_logs(path: str) -> list[tuple[datetime, str]]:
    with open(path) as fd:
        rows = csv.reader(fd, delimiter='\t')
        queries = [(_to_datetime(row[0]), row[1]) for row in rows]
    return queries


print('Started reading the csv')
start = time.time()
queries = parse_logs('hn_logs.tsv')
end = time.time()
print(end - start)
print('Finished reading the csv')


count_router = APIRouter(prefix='/queries/count')

@count_router.get('/{date}', response_model=int | str, status_code=200)
async def count(date: str, response: Response)  -> int | str:
    try:
        d = datetime.strptime(date, '%Y')
        matching_queries = [q[1] for q in queries if q[0].year == d.year]
        return len(set(matching_queries))
    except ValueError as e:
        pass

    try:
        d = datetime.strptime(date, '%Y-%m')
        matching_queries = [q[1] for q in queries if q[0].year == d.year \
            and q[0].month == d.month]
        return len(set(matching_queries)) 
    except ValueError:
        pass

    try:
        d = datetime.strptime(date, '%Y-%m-%d')
        matching_queries = [q[1] for q in queries if q[0].year == d.year \
            and q[0].month == d.month and q[0].day == d.day]
        return len(set(matching_queries)) 
    except ValueError:
        pass

    try:
        d = datetime.strptime(date, '%Y-%m-%d %H:%M')
        matching_queries = [q[1] for q in queries if q[0].year == d.year \
            and q[0].month == d.month and q[0].day == d.day and q[0].hour == d.hour and q[0].minute == d.minute]
        return len(set(matching_queries)) 
    except ValueError:
        pass
    
    response.status_code = 400
    return 'Incorrect date format. The following formats are accepted: \
            Y, Y-m, Y-m-d, Y-m-d H:M:s \
            (for example: 2015, 2015-08-03 2015-08-03 00:04)'


app.include_router(count_router)


@app.get('/')
async def root():
    return {'message': 'It works'}
