#!/bin/bash

# kafka broker:
server=localhost:9092

# data sourced from mariadb relational.fit.cvut.cz:3306 guest:password

# course:
#select concat(cast(c.course_id as char), ':', cast(
#    json_object(
#        'courseId', c.course_id,
#        'courseLevel', c.courseLevel,
#        'taughtBy',
#            json_merge('[]',
#                       concat('[', group_concat(json_object('personId', tB.p_id)), ']')
#            )
#    )
#    as char)
#) as `json`
#from course c
#left join taughtBy tB on c.course_id = tB.course_id
#group by c.course_id, c.courseLevel;

# person:
#select concat(cast(p.p_id as char), ':', cast(json_object(
#        'personId', p.p_id,
#        'professor', not not convert(p.professor, integer),
#        'student', not not convert(p.student, integer),
#        'hasPosition', not not convert(p.hasPosition, integer),
#        'inPhase', not not convert(p.inPhase, integer),
#        'yearsInProgram', p.yearsInProgram,
#        'advisedBy', json_merge('[]',
#            concat('[', group_concat(json_object('personId', aB.p_id_dummy)), ']')
#        )
#    ) as char
#)) as `json`
#from person p
#left join advisedBy aB on p.p_id = aB.p_id
#group by p.p_id, p.professor, p.student, p.hasPosition, p.inPhase, p.yearsInProgram;

# delete existing topics:
echo delete cr_course
kafka-topics --bootstrap-server $server --delete --topic cr_course
echo delete cr_person
kafka-topics --bootstrap-server $server --delete --topic cr_person

# create topics:
echo create cr_course
kafka-topics --bootstrap-server $server --create --topic cr_course --partitions 12
echo create cr_person
kafka-topics --bootstrap-server $server --create --topic cr_person --partitions 12

# produce data:
echo produce to cr_course
kafkacat -b $server -P -l -K: -t cr_course course.json
echo produce to cr_person
kafkacat -b $server -P -l -K: -t cr_person person.json

echo done
