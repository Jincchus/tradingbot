from db.database import Base, create_engine_for_process
from db import models  # noqa: F401

if __name__ == "__main__":
    engine = create_engine_for_process()
    Base.metadata.create_all(engine)
    print("Tables created.")
