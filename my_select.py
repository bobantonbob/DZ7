from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_1():
    """
    SELECT
    s.id,
    s.fullname,
    ROUND(AVG(g.grade), 2) AS average_grade
FROM students s
JOIN grades g ON s.id = g.student_id
GROUP BY s.id
ORDER BY average_grade DESC
LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_2():
    """
SELECT
    s.id,
    s.fullname,
    ROUND(AVG(g.grade), 2) AS average_grade
FROM grades g
JOIN students s ON s.id = g.student_id
where g.subject_id = 1
GROUP BY s.id
ORDER BY average_grade DESC
LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id =3
    );
    :return:
    """
    subquery = (select(func.max(Grade.grade_date))).join(Student).filter(
        and_(Grade.subjects_id == 2, Student.group_id == 3)).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade).join(Student).filter(
        and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


def select_3():
    """
    SELECT
      g.name AS group_name,
      s.name AS subject_name,
      AVG(grade) AS average_grade
    FROM
      grades gr
    JOIN
      students st ON gr.student_id = st.id
    JOIN
      groups g ON st.group_id = g.id
    JOIN
      subjects s ON gr.subject_id = s.id
    WHERE
      s.name = 'game' --'Назва певного предмета'
    GROUP BY
      g.name, s.name;уймо його перевести в запит ORM SQLAlchemy. Нехай у нас є сесія у змінній session. Є описані моделі Student та Grade для відповідних таблиць.

    """
    result = (
        session.query(
            Group.name.label('group_name'),
            Subject.name.label('subject_name'),
            func.avg(Grade.grade).label('average_grade')
        )
        .join(Student, Grade.student_id == Student.id)
        .join(Group, Student.group_id == Group.id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .filter(Subject.name == 'game')
        .group_by(Group.name, Subject.name)
        .all()
    )
    return result


def select_4():
    """
    SELECT AVG(grade) AS average_grade
    FROM grades;
    :return:
    """
    result = (
        session.query(func.avg(Grade.grade).label('average_grade'))
        .scalar()
    )
    return result


def select_5():
    """
    SELECT
      t.fullname AS teacher_name,
      s.name AS subject_name
    FROM
      teachers t
    JOIN
      subjects s ON t.id = s.teacher_id
    WHERE
      t.fullname = 'Matthew Rodriguez';  -- ПІБ певного викладача
    :return:
    """
    result = (
        session.query(
            Teacher.fullname.label('teacher_name'),
            Subject.name.label('subject_name')
        )
        .join(Subject, Teacher.id == Subject.teacher_id)
        .filter(Teacher.fullname == 'Matthew Rodriguez')
        .all()
    )
    return result



def select_6():
    """
    SELECT
      s.fullname AS student_name,
      g.name AS group_name
    FROM
      students s
    JOIN
      groups g ON s.group_id = g.id
    WHERE
      g.name = 'well'; -- Назва певної групи
    :return:
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Group.name.label('group_name')
        )
        .join(Group, Student.group_id == Group.id)
        .filter(Group.name == 'well')
        .all()
    )
    return result


def select_7():
    """
    SELECT
      s.fullname AS student_name,
      g.name AS group_name,
      sb.name AS subject_name,
      gr.grade,
      gr.grade_date
    FROM
      grades gr
    JOIN
      students s ON gr.student_id = s.id
    JOIN
      groups g ON s.group_id = g.id
    JOIN
      subjects sb ON gr.subject_id = sb.id
    WHERE
      g.name = 'rate' -- Назва певної групи
      AND sb.name = 'game'; -- Назва певного предмета
    """

    result = (
        session.query(
            Student.fullname.label('student_name'),
            Group.name.label('group_name'),
            Subject.name.label('subject_name'),
            Grade.grade,
            Grade.grade_date
        )
        .join(Student, Grade.student_id == Student.id)
        .join(Group, Student.group_id == Group.id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .filter(Group.name == 'rate')
        .filter(Subject.name == 'game')
        .all()
    )
    return result

def select_8():
    """
    SELECT
      t.fullname AS teacher_name,
      AVG(gr.grade) AS average_grade
    FROM
      teachers t
    JOIN
      subjects s ON t.id = s.teacher_id
    JOIN
      grades gr ON s.id = gr.subject_id
    WHERE
      t.fullname = 'Nicolas Delacruz' -- ПІБ певного викладача
    GROUP BY
      t.fullname;
    :return:
    """
    result = (
        session.query(
            Teacher.fullname.label('teacher_name'),
            func.avg(Grade.grade).label('average_grade')
        )
        .join(Subject, Teacher.id == Subject.teacher_id)
        .join(Grade, Subject.id == Grade.subjects_id)
        .filter(Teacher.fullname == 'Nicolas Delacruz')
        .group_by(Teacher.fullname)
        .all()
    )
    return result


def select_9():
    """
    SELECT DISTINCT
      s.fullname AS student_name,
      g.name AS group_name,
      sb.name AS subject_name
    FROM
      students s
    JOIN
      groups g ON s.group_id = g.id
    JOIN
      grades gr ON s.id = gr.student_id
    JOIN
      subjects sb ON gr.subject_id = sb.id
    WHERE
      s.fullname = 'Mr. James Reynolds'; -- ПІБ певного студента
    :return:
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Group.name.label('group_name'),
            Subject.name.label('subject_name')
        )
        .join(Group, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .filter(Student.fullname == 'Mr. James Reynolds')
        .distinct()
        .all()
    )

    return result


def select_10():
    """
    SELECT DISTINCT
      s.fullname AS student_name,
      g.name AS group_name,
      sb.name AS subject_name,
      t.fullname AS teacher_name
    FROM
      students s
    JOIN
      groups g ON s.group_id = g.id
    JOIN
      grades gr ON s.id = gr.student_id
    JOIN
      subjects sb ON gr.subject_id = sb.id
    JOIN
      teachers t ON sb.teacher_id = t.id
    WHERE
      s.fullname = 'Mr. James Reynolds' and  -- ПІБ певного студента
      t.fullname = 'Matthew Rodriguez';   -- ПІБ певного викладача
    :return:
    """
    result = (
        session.query(
            Student.fullname.label('student_name'),
            Group.name.label('group_name'),
            Subject.name.label('subject_name'),
            Teacher.fullname.label('teacher_name')
        )
        .join(Group, Student.group_id == Group.id)
        .join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subjects_id == Subject.id)
        .join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.fullname == 'Mr. James Reynolds')
        .filter(Teacher.fullname == 'Matthew Rodriguez')
        .distinct()
        .all()
    )
    return result



if __name__ == '__main__':
    # print(select_1())
    # print(select_2())
    # print(select_3())
    # print(select_4())
    # print(select_5())
    # print(select_6())
    # print(select_7())
    # print(select_8())
    # print(select_9())
    print(select_10())
    # print(select_12())
