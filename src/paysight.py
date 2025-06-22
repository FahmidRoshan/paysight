from data.gtl import run_all
from db.schema_initializer import create_tables

def paysight():
    print("Paysight")
    create_tables()
    run_all()

if __name__ == "__main__":
    paysight()