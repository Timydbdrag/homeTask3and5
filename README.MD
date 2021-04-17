Home task 1

1. ДЗ - 3     


    -задача 1    

sbt runMain exercise.TaskOne
результат выведется на экран и появится директория outInfo   
     

    -звдача 2
sbt runMain exercise.TaskTwo
результат выведется на экран и появистся файл test.txt

    -задача 3
необходимо запустить БД 

начакить схему командой sbt flywayMigrate

при необходимости, можно зачистить БД командой  sbt flywayClean (ВАЖНО! удалит все таблицы)

либо создать таблицу через консоль БД, схема тут -> src/main/resources/db/migration/V1__testDataMart.sql

sbt runMain exercise.TaskThree

результат выведется на экран и запишется в БД



2.ДЗ - 5 

тесты тут -> src/test