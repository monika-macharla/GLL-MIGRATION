from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, insert
import datetime

def setup():
    # Source DB
    source_engine = create_engine('sqlite:///source.db')
    metadata_source = MetaData()
    
    source_users = Table('source_users', metadata_source,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('email', String),
        Column('created_at', DateTime, default=datetime.datetime.utcnow)
    )
    
    metadata_source.drop_all(source_engine) # Clean start
    metadata_source.create_all(source_engine)
    
    with source_engine.begin() as conn:
        conn.execute(insert(source_users), [
            {'name': 'John Doe', 'email': 'john@example.com'},
            {'name': 'Jane Smith', 'email': 'jane@example.com'},
            {'name': 'Bob Johnson', 'email': 'bob@example.com'}
        ])
    print("Source database 'source.db' created with sample data.")

    # Destination DB
    dest_engine = create_engine('sqlite:///destination.db')
    metadata_dest = MetaData()
    
    dest_users = Table('dest_users', metadata_dest,
        Column('user_id', Integer, primary_key=True),
        Column('full_name', String),
        Column('email', String),
        Column('date_joined', DateTime)
    )
    
    metadata_dest.drop_all(dest_engine) # Clean start
    metadata_dest.create_all(dest_engine)
    print("Destination database 'destination.db' created with target structure.")

if __name__ == "__main__":
    setup()
