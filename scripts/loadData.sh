#!/bin/bash
#######################################
# load q2 data
#######################################
cd /home/ubuntu/q2data
for f in *
do
sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "
      LOAD DATA LOCAL INFILE '$f'
      INTO TABLE test2 
      FIELDS TERMINATED BY '\t' 
      LINES TERMINATED BY '\n';"
        echo "$f finish" 
done

sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "CREATE INDEX useridIndex ON test2 (userid,newDate);"
echo create index q2 finish

echo finishQ2
#######################################
# load q3 data
#######################################
cd /home/ubuntu/q3data
for f in *
do
sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "
      LOAD DATA LOCAL INFILE '$f'
      INTO TABLE test3 
      FIELDS TERMINATED BY '\t' 
      LINES TERMINATED BY '\n';"
        echo "$f finish" 
done

sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "CREATE INDEX useridIndex ON test3 (userid);"
echo create index q3 finish 

echo finishQ3

#######################################
# load q4 data
#######################################
cd /home/ubuntu/q4data
for f in *
do
sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "
      LOAD DATA LOCAL INFILE '$f'
      INTO TABLE test4
      FIELDS TERMINATED BY '\t' 
      LINES TERMINATED BY '\n';"
        echo "$f finish" 
done

sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "CREATE INDEX tagIndex ON test4 (hashTag);"
echo create index q4 finish

echo finishQ4
#######################################
# load q5 data
#######################################
cd /home/ubuntu/q5data
for f in *
do
sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "
      LOAD DATA LOCAL INFILE '$f'
      INTO TABLE test5
      FIELDS TERMINATED BY '\t' 
      LINES TERMINATED BY '\n';"
        echo "$f finish" 
done

sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "CREATE INDEX idIndex ON test5 (userid);"
echo create index q5 finish

echo finishQ5


#######################################
# load q6 data
#######################################
cd /home/ubuntu/q6data
for f in *
do
sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "
      LOAD DATA LOCAL INFILE '$f'
      INTO TABLE test6
      FIELDS TERMINATED BY '\t' 
      LINES TERMINATED BY '\n';"
        echo "$f finish" 
done

sudo mysql --local-infile -uroot -p123456 mydb -e "use mydb" -e "CREATE INDEX tweetidIndex ON test6 (tweetid);"
echo create index q6 finish
echo finishQ6

