from sqlalchemy import create_engine, select, insert, delete, update
from sqlalchemy.orm import sessionmaker, scoped_session
from os import getenv
from models.base_model import Base
from models.user import User
from models.place import Place
from models.state import State
from models.city import City
from models.amenity import Amenity
from models.review import Review

HBNB_MYSQL_DB= getenv("HBNB_MYSQL_DB")
HBNB_MYSQL_USER=getenv("HBNB_MYSQL_USER")
HBNB_MYSQL_PWD=getenv("HBNB_MYSQL_PWD")
HBNB_MYSQL_HOST=getenv("HBNB_MYSQL_HOST")
HBNB_ENV=getenv("HBNB_ENV")

class DBStorage:
    __engine = None
    __session = None
    __tables = {
        "User": User,
        "Place": Place,
        "State": State,
        "City": City,
        "Amenity": Amenity,
        "Review": Review
    }

    def __init__(self):
        config = f"mysql://{HBNB_MYSQL_USER}:{HBNB_MYSQL_PWD}@{HBNB_MYSQL_HOST}/{HBNB_MYSQL_DB}"
        DBStorage.__engine = create_engine(config, echo=True, pool_pre_ping=True)

        if HBNB_ENV == "test":
            Base.metadata.drop_all(bind=DBStorage.__engine)

    def all(self, cls=None):
        rows = {}

        if cls == None:
            for table in DBStorage.__tables.values():
                rows = {**rows, **self.__get_rows(table)}

        else:
            rows = self.__get_rows(DBStorage.__tables[cls])
        return rows
            

    def new(self, obj):
        # c_name = obj.__class__.__name__

        # stmt = insert(DBStorage.__tables[c_name]).values(**obj.to_dict())
        # DBStorage.__session.execute(stmt)
        DBStorage.__session.add(obj)


    def save(self):
        DBStorage.__session.flush()
        DBStorage.__session.commit()

    def delete(self, obj=None):
        if obj == None:
            return

        c_name = obj.__class__.__name__
        id = obj.id
        table = DBStorage.__tables[c_name]
        stmt = delete(table).where(table.id == id)
        DBStorage.__session.execute(stmt)

        # DBStorage.__session.delete(obj)

    def reload(self):
        Base.metadata.create_all(DBStorage.__engine)

        session_factory = sessionmaker(bind=DBStorage.__engine, expire_on_commit=False)
        Session = scoped_session(session_factory)

        DBStorage.__session = Session()

    def __get_rows(self, table):
        stmt = select(table)
        rows = {}
        for row in self.__session.scalars(stmt):
                key = f"{row.__class__.__name__}.{row.id}"
                rows[key] = row

        return rows