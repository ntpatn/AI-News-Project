from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Text, String, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


class StgNewsArticles(Base):
    __tablename__ = "stg_news_articles"
    __table_args__ = {"schema": "silver"}  # ตรงกับ silver.stg_news_articles

    id = Column(UUID(as_uuid=True), primary_key=True)
    publishedtime = Column(DateTime(timezone=True))
    titlenews = Column(Text)
    description = Column(Text)
    imageurl = Column(Text)
    url = Column(Text)
    category = Column(String(50))
    source = Column(Text)
    author = Column(Text)
    language = Column(Text)
    createdate = Column(DateTime(timezone=True))
    usercreate = Column(Text)
    updatedate = Column(DateTime(timezone=True))
    userupdate = Column(Text)
    activedata = Column(Boolean)
