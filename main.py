import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from conf.models import Grade, Teacher, Student, Group, Subject


# Функція для створення сесії бази даних
def create_session():
    engine = create_engine('postgresql://postgres:567234@localhost/DZ6.3', echo=True)
    Session = sessionmaker(bind=engine)
    return Session()


# Функція для додавання нового запису до бази даних
def create_record(session, model, **kwargs):
    if 'name' in kwargs:
        if 'fullname' in model.__table__.columns:
            new_record = model(fullname=kwargs['name'])
        elif 'name' in model.__table__.columns:
            new_record = model(name=kwargs['name'])
        else:
            print(f"Model {model.__name__} does not have 'fullname' or 'name' field.")
            return
        session.add(new_record)
        session.commit()
    else:
        print("Name parameter is missing.")


# Функція для виведення всіх записів моделі
def list_records(session, model):
    records = session.query(model).all()
    for record in records:
        print(record)


# показати всіх вчителів
def list_teachers(session: Session):
    teachers = session.query(Teacher).all()
    for teacher in teachers:
        print(teacher)


# Функція для оновлення запису за ідентифікатором
def update_record(session: Session, model, record_id, **kwargs):
    record = session.query(model).filter_by(id=record_id).first()
    if record:
        for key, value in kwargs.items():
            setattr(record, key, value)
        session.commit()
        print(f"{model.__name__} with ID {record_id} has been successfully updated.")
    else:
        print(f"{model.__name__} with ID {record_id} not found.")# Функція для видалення запису за ідентифікатором
def remove_record(session, model, record_id):
    record = session.query(model).get(record_id)
    if record:
        session.delete(record)
        session.commit()
    else:
        print(f"{model.__name__} with ID {record_id} not found.")


def main():
    parser = argparse.ArgumentParser(description='Perform CRUD operations on the database.')
    parser.add_argument('--action', '-a', choices=['create', 'list', 'update', 'remove'], required=True,
                        help='CRUD operation')
    parser.add_argument('--model', '-m', choices=['Teacher', 'Group', 'Student'], required=True,
                        help='Model to perform operation on')
    parser.add_argument('--id', type=int, help='Record ID for update or remove operations')
    parser.add_argument('--name', help='Name for create or update operations')

    args = parser.parse_args()

    session = create_session()

    if args.action == 'create':
        create_record(session, globals()[args.model], name=args.name)
    elif args.action == 'list':
        list_records(session, globals()[args.model])
    elif args.action == 'update':
        update_record(session, globals()[args.model], args.id, name=args.name)
    elif args.action == 'remove':
        remove_record(session, globals()[args.model], args.id)


if __name__ == "__main__":
    main()
