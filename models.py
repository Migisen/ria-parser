import traceback
from contextlib import contextmanager

from sqlalchemy import create_engine, orm, Column, String, DateTime, Text, Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql+psycopg2://nir:nir@/nir', echo=False)
Base = declarative_base()


class RiaNews(Base):
    __tablename__ = 'ria_news'

    id = Column(Integer, primary_key=True)

    title = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    url = Column(String, nullable=False)
    url_next = Column(String, nullable=True)
    tags = Column(ARRAY(String, dimensions=1))
    text = Column(Text, nullable=False)

    def __str(self):
        return f'<RiaNews(title={self.title}, date={self.date}, url={self.url}, ' \
               f'url_next={self.url_next}, tags={self.tags}, text={self.text})>'

    def __str__(self):
        return self.__str()

    def __repr__(self):
        return self.__str()


def init_db():
    """Создает глобальную сессию."""
    global Session
    Session = orm.sessionmaker(bind=engine, expire_on_commit=False)
    Base.metadata.create_all(engine)


@contextmanager
def session_scope():
    """Удобная обертка для открытия безопасного сессий БД."""
    session: orm.Session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        #raise
        print(f'Failed to add to DB!')
        traceback.print_exc()
    finally:
        session.close()
