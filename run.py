import json

from graphwalker_sqlalchemy import extract
from models import BaseModel


if __name__ == '__main__':
    graph = extract(BaseModel)
    print(json.dumps(graph, indent=1))
